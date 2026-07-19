// Package server implements algovn.button.v2.ButtonService (spec §4, §6).
package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	buttonv2 "github.com/the-algovn/protos/gen/go/algovn/button/v2"
	"github.com/the-algovn/the-button-service/internal/achievements"
	"github.com/the-algovn/the-button-service/internal/clicks"
	"github.com/the-algovn/the-button-service/internal/difficulty"
	"github.com/the-algovn/the-button-service/internal/leaderboard"
	"github.com/the-algovn/the-button-service/internal/pow"
	"github.com/the-algovn/the-button-service/internal/quests"
	"github.com/the-algovn/the-button-service/internal/streak"
)

var hcm = func() *time.Location {
	loc, err := time.LoadLocation("Asia/Ho_Chi_Minh")
	if err != nil {
		panic(err)
	}
	return loc
}()

// dateHCM is the Asia/Ho_Chi_Minh calendar date (YYYY-MM-DD) for t.
func dateHCM(t time.Time) string { return t.In(hcm).Format("2006-01-02") }

type Server struct {
	buttonv2.UnimplementedButtonServiceServer
	RDB    *redis.Client
	Prod   *kgo.Client
	Diff   *difficulty.Cache
	Logger *slog.Logger
	W0     uint64
	Keys   [][]byte // [current] or [current, previous] — rotation window
}

// subFromContext does the read-only segment-2 decode of the forwarded JWT
// per authnz-conventions.md — the gateway already verified the signature
// and is the sole verified ingress.
func subFromContext(ctx context.Context) (string, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return "", errors.New("no authorization metadata")
	}
	parts := strings.Split(strings.TrimPrefix(vals[0], "Bearer "), ".")
	if len(parts) != 3 {
		return "", errors.New("not a JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", errors.New("bad JWT payload")
	}
	var claims struct {
		Sub string `json:"sub"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Sub == "" {
		return "", errors.New("bad claims")
	}
	return claims.Sub, nil
}

// identityFromContext does the single read-only segment-2 decode of the
// forwarded, gateway-verified JWT and returns the subject plus a
// server-derived display name (name -> preferred_username -> clicker-<sub6>).
// The SPA never supplies the name. This replaces separate sub/displayName
// decodes on the submit path.
func identityFromContext(ctx context.Context) (sub, displayName string, err error) {
	md, _ := metadata.FromIncomingContext(ctx)
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return "", "", errors.New("no authorization metadata")
	}
	parts := strings.Split(strings.TrimPrefix(vals[0], "Bearer "), ".")
	if len(parts) != 3 {
		return "", "", errors.New("not a JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", "", errors.New("bad JWT payload")
	}
	var c struct {
		Sub               string `json:"sub"`
		Name              string `json:"name"`
		PreferredUsername string `json:"preferred_username"`
	}
	if err := json.Unmarshal(payload, &c); err != nil || c.Sub == "" {
		return "", "", errors.New("bad claims")
	}
	switch {
	case c.Name != "":
		displayName = c.Name
	case c.PreferredUsername != "":
		displayName = c.PreferredUsername
	default:
		displayName = "clicker-" + c.Sub[:min(6, len(c.Sub))]
	}
	return c.Sub, displayName, nil
}

func (s *Server) GetCounter(ctx context.Context, _ *buttonv2.GetCounterRequest) (*buttonv2.GetCounterResponse, error) {
	total, err := s.RDB.Get(ctx, "counter:total").Uint64()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, status.Error(codes.Unavailable, "counter unavailable")
	}
	users, err := s.RDB.ZCard(ctx, leaderboard.AllTimeKey).Result()
	if err != nil {
		return nil, status.Error(codes.Unavailable, "counter unavailable")
	}
	return &buttonv2.GetCounterResponse{Total: total, TotalUsers: uint64(users)}, nil
}

// issue builds a signed challenge for sub from the shared difficulty cache.
// Fails closed: no cached value yet → Unavailable — no local defaults, the
// publisher owns pow:L / pow:min_interval (spec §5).
func (s *Server) issue(sub string) (*buttonv2.IssueChallengeResponse, error) {
	l, minInterval, ok := s.Diff.Get()
	if !ok {
		return nil, status.Error(codes.Unavailable, "difficulty unavailable")
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, status.Error(codes.Internal, "uuid")
	}
	now := time.Now()
	p := pow.Payload{
		ID:           id.String(),
		Sub:          sub,
		Iat:          now.Unix(),
		Exp:          now.Add(pow.TokenTTL).Unix(),
		W0:           s.W0,
		L:            uint32(l),
		MinIntervalS: uint32(minInterval),
		MaxBatch:     pow.MaxBatch,
	}
	tok, err := pow.Sign(p, s.Keys[0])
	if err != nil {
		return nil, status.Error(codes.Internal, "sign")
	}
	return &buttonv2.IssueChallengeResponse{
		Challenge:          tok,
		WorkFactor:         s.W0 * l,
		MinIntervalSeconds: uint32(minInterval),
		MaxBatch:           pow.MaxBatch,
		ExpiresAt:          timestamppb.New(time.Unix(p.Exp, 0)),
	}, nil
}

func (s *Server) IssueChallenge(ctx context.Context, _ *buttonv2.IssueChallengeRequest) (*buttonv2.IssueChallengeResponse, error) {
	sub, err := subFromContext(ctx)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	return s.issue(sub)
}

func (s *Server) SubmitClicks(ctx context.Context, req *buttonv2.SubmitClicksRequest) (*buttonv2.SubmitClicksResponse, error) {
	sub, displayName, err := identityFromContext(ctx)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	p, err := pow.Parse(req.GetChallenge(), s.Keys...)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "bad challenge")
	}
	now := time.Now()
	if err := pow.Verify(p, sub, now); err != nil {
		if errors.Is(err, pow.ErrExpired) {
			return nil, status.Error(codes.FailedPrecondition, "challenge_expired")
		}
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	count := req.GetClickCount()
	if count == 0 || count > p.MaxBatch {
		return nil, status.Errorf(codes.InvalidArgument, "click_count must be 1..%d", p.MaxBatch)
	}
	if !pow.CheckWork(req.GetChallenge(), p.W0, p.L, count, req.GetNonce()) {
		return nil, status.Error(codes.InvalidArgument, "bad proof of work")
	}

	if err := clicks.Submit(ctx, s.RDB, s.Prod, p, count, now, displayName); err != nil {
		return nil, err
	}
	resp := &buttonv2.SubmitClicksResponse{}
	// piggyback the next challenge; issuance failure must not void the batch
	if next, err := s.issue(sub); err == nil {
		resp.NextChallenge = next
	} else {
		s.Logger.Warn("piggyback issue failed", "err", err)
	}
	return resp, nil
}

func (s *Server) GetLeaderboard(ctx context.Context, _ *buttonv2.GetLeaderboardRequest) (*buttonv2.GetLeaderboardResponse, error) {
	now := time.Now()
	weekKey := leaderboard.WeekKey(now)
	resp := &buttonv2.GetLeaderboardResponse{
		AllTime:  s.renderBoard(ctx, leaderboard.AllTimeKey),
		ThisWeek: s.renderBoard(ctx, weekKey),
	}
	if sub, err := subFromContext(ctx); err == nil {
		resp.MyAllTimeRank = leaderboard.Rank(ctx, s.RDB, leaderboard.AllTimeKey, sub)
		resp.MyWeeklyRank = leaderboard.Rank(ctx, s.RDB, weekKey, sub)
	}
	return resp, nil
}

func (s *Server) renderBoard(ctx context.Context, key string) []*buttonv2.LeaderboardEntry {
	top, err := leaderboard.TopN(ctx, s.RDB, key, 20)
	if err != nil || len(top) == 0 {
		return nil
	}
	subs := make([]string, len(top))
	for i, r := range top {
		subs[i] = r.Sub
	}
	names := map[string]string{}
	if vals, err := s.RDB.HMGet(ctx, "profile:names", subs...).Result(); err == nil {
		for i, v := range vals {
			if str, ok := v.(string); ok {
				names[subs[i]] = str
			}
		}
	}
	out := make([]*buttonv2.LeaderboardEntry, len(top))
	for i, r := range top {
		name := names[r.Sub]
		if name == "" {
			name = "clicker-" + r.Sub[:min(6, len(r.Sub))]
		}
		out[i] = &buttonv2.LeaderboardEntry{Rank: uint32(i + 1), DisplayName: name, Clicks: r.Clicks}
	}
	return out
}

func (s *Server) GetPlayerState(ctx context.Context, _ *buttonv2.GetPlayerStateRequest) (*buttonv2.GetPlayerStateResponse, error) {
	sub, err := subFromContext(ctx)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	now := time.Now()
	date := dateHCM(now)
	weekStart := leaderboard.WeekStartString(now)
	weekKey := leaderboard.WeekKey(now)

	resp := &buttonv2.GetPlayerStateResponse{
		TotalClicks: uint64(s.RDB.ZScore(ctx, leaderboard.AllTimeKey, sub).Val()),
		AllTimeRank: rankOf(ctx, s.RDB, leaderboard.AllTimeKey, sub),
		WeeklyRank:  rankOf(ctx, s.RDB, weekKey, sub),
	}

	// achievements: full catalog, unlocked_at from the unlocks hash (one round trip).
	ids := make([]string, len(achievements.Catalog))
	for i, a := range achievements.Catalog {
		ids[i] = a.ID
	}
	unlockedAt := map[string]int64{}
	if vals, err := s.RDB.HMGet(ctx, "unlocks:"+sub, ids...).Result(); err == nil {
		for i, v := range vals {
			if str, ok := v.(string); ok {
				if ts, perr := strconv.ParseInt(str, 10, 64); perr == nil {
					unlockedAt[ids[i]] = ts
				}
			}
		}
	}
	for _, a := range achievements.Catalog {
		pa := &buttonv2.Achievement{Id: a.ID, Title: a.Title, Description: a.Description}
		if ts, ok := unlockedAt[a.ID]; ok {
			pa.UnlockedAt = timestamppb.New(time.Unix(ts, 0))
		}
		resp.Achievements = append(resp.Achievements, pa)
	}

	// reached milestones (Redis; missing/err → omit that milestone).
	mkeys := make([]string, len(achievements.Milestones))
	for i, m := range achievements.Milestones {
		mkeys[i] = fmt.Sprintf("milestone:%d", m.Threshold)
	}
	if vals, err := s.RDB.MGet(ctx, mkeys...).Result(); err == nil {
		for i, v := range vals {
			str, ok := v.(string)
			if !ok {
				continue
			}
			ts, perr := strconv.ParseInt(str, 10, 64)
			if perr != nil {
				continue
			}
			m := achievements.Milestones[i]
			resp.Milestones = append(resp.Milestones, &buttonv2.Milestone{
				Threshold: m.Threshold, Title: m.Title, ReachedAt: timestamppb.New(time.Unix(ts, 0)),
			})
		}
	}

	// quests: active daily+weekly with this caller's progress.
	sig := s.gatherSignals(ctx, sub, date, weekStart, weekKey)
	dailyReset := timestamppb.New(nextMidnightHCM(now))
	weeklyReset := timestamppb.New(leaderboard.WeekStart(now).AddDate(0, 0, 7))
	for _, d := range quests.DailyQuests(date) {
		resp.Quests = append(resp.Quests, questProto(d, sig, dailyReset))
	}
	for _, d := range quests.WeeklyQuests(weekStart) {
		resp.Quests = append(resp.Quests, questProto(d, sig, weeklyReset))
	}

	// streak
	st := loadStreak(ctx, s.RDB, sub)
	resp.Streak = &buttonv2.Streak{
		CurrentDays: st.Count, BestDays: st.Best, LastContribDate: st.LastDay,
	}
	return resp, nil
}

func (s *Server) gatherSignals(ctx context.Context, sub, date, weekStart, weekKey string) quests.Signals {
	dk := "daily:" + sub + ":" + date
	wk := "weekdays:" + sub + ":" + weekStart
	clicksToday, _ := s.RDB.HGet(ctx, dk, "clicks").Uint64()
	batchesToday, _ := s.RDB.HGet(ctx, dk, "batches").Uint64()
	maxBatchToday, _ := s.RDB.HGet(ctx, dk, "maxbatch").Uint64()
	daysThisWeek, _ := s.RDB.SCard(ctx, wk).Result()
	return quests.Signals{
		ClicksToday: clicksToday, BatchesToday: batchesToday, MaxBatchToday: maxBatchToday,
		DaysThisWeek:   uint64(daysThisWeek),
		ClicksThisWeek: uint64(s.RDB.ZScore(ctx, weekKey, sub).Val()),
		WeeklyRank:     rankOf(ctx, s.RDB, weekKey, sub),
	}
}

func questProto(d quests.Def, sig quests.Signals, resetsAt *timestamppb.Timestamp) *buttonv2.Quest {
	p, done := quests.Progress(d, sig)
	kind := buttonv2.QuestKind_QUEST_KIND_DAILY
	if d.Kind == quests.Weekly {
		kind = buttonv2.QuestKind_QUEST_KIND_WEEKLY
	}
	return &buttonv2.Quest{
		Id: d.ID, Title: d.Title, Description: d.Description, Kind: kind,
		Target: d.Target, Progress: p, Done: done, Reward: d.Reward, ResetsAt: resetsAt,
	}
}

// rankOf is the 1-based ZSET rank (highest score = 1), 0 if unranked.
func rankOf(ctx context.Context, rdb *redis.Client, key, sub string) uint32 {
	r, err := rdb.ZRevRank(ctx, key, sub).Result()
	if err != nil {
		return 0
	}
	return uint32(r) + 1
}

// nextMidnightHCM is the next 00:00 Asia/Ho_Chi_Minh strictly after t.
func nextMidnightHCM(t time.Time) time.Time {
	l := t.In(hcm)
	y, m, d := l.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, hcm).AddDate(0, 0, 1)
}

// loadStreak reads the per-user streak hash (count/best/lastday).
func loadStreak(ctx context.Context, rdb *redis.Client, sub string) streak.State {
	vals, err := rdb.HGetAll(ctx, "streak:"+sub).Result()
	if err != nil || len(vals) == 0 {
		return streak.State{}
	}
	count, _ := strconv.ParseUint(vals["count"], 10, 32)
	best, _ := strconv.ParseUint(vals["best"], 10, 32)
	return streak.State{Count: uint32(count), Best: uint32(best), LastDay: vals["lastday"]}
}
