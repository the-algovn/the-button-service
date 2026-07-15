// Package server implements algovn.button.v1.ButtonService (spec §4, §6).
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

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	buttonv1 "github.com/the-algovn/protos/gen/go/algovn/button/v1"
	"github.com/the-algovn/the-button-service/internal/achievements"
	"github.com/the-algovn/the-button-service/internal/clicks"
	"github.com/the-algovn/the-button-service/internal/db"
	"github.com/the-algovn/the-button-service/internal/pow"
)

// Totaler is the per-replica cached counter (ticker.Ticker).
type Totaler interface {
	Total() (uint64, bool)
	Users() (uint64, bool)
}

type Server struct {
	buttonv1.UnimplementedButtonServiceServer
	Pool   *pgxpool.Pool
	RDB    *redis.Client
	Tick   Totaler
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

func (s *Server) GetCounter(context.Context, *buttonv1.GetCounterRequest) (*buttonv1.GetCounterResponse, error) {
	total, ok := s.Tick.Total()
	if !ok {
		return nil, status.Error(codes.Unavailable, "counter not warmed up")
	}
	users, _ := s.Tick.Users() // 0 until warmed → protojson omits → client shows —
	return &buttonv1.GetCounterResponse{Total: total, TotalUsers: users}, nil
}

// issue builds a signed challenge for sub from the shared difficulty keys.
// Fails closed: Redis miss/error → Unavailable — no local defaults, the
// leader owns pow:L / pow:min_interval (spec §5).
func (s *Server) issue(ctx context.Context, sub string) (*buttonv1.IssueChallengeResponse, error) {
	l, err := s.RDB.Get(ctx, "pow:L").Uint64()
	if err != nil {
		return nil, status.Error(codes.Unavailable, "difficulty unavailable")
	}
	minInterval, err := s.RDB.Get(ctx, "pow:min_interval").Uint64()
	if err != nil {
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
	return &buttonv1.IssueChallengeResponse{
		Challenge:          tok,
		WorkFactor:         s.W0 * l,
		MinIntervalSeconds: uint32(minInterval),
		MaxBatch:           pow.MaxBatch,
		ExpiresAt:          timestamppb.New(time.Unix(p.Exp, 0)),
	}, nil
}

func (s *Server) IssueChallenge(ctx context.Context, _ *buttonv1.IssueChallengeRequest) (*buttonv1.IssueChallengeResponse, error) {
	sub, err := subFromContext(ctx)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	return s.issue(ctx, sub)
}

func (s *Server) SubmitClicks(ctx context.Context, req *buttonv1.SubmitClicksRequest) (*buttonv1.SubmitClicksResponse, error) {
	sub, err := subFromContext(ctx)
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

	res, err := clicks.Submit(ctx, s.RDB, s.Pool, s.Logger, p, count, now)
	if err != nil {
		return nil, err
	}

	resp := &buttonv1.SubmitClicksResponse{UserTotalClicks: res.UserTotal}
	for _, u := range res.Unlocked {
		resp.Unlocked = append(resp.Unlocked, &buttonv1.Achievement{
			Id:          u.Achievement.ID,
			Title:       u.Achievement.Title,
			Description: u.Achievement.Description,
			UnlockedAt:  timestamppb.New(u.UnlockedAt),
		})
	}
	// piggyback the next challenge; issuance failure must not void the
	// accepted batch (spec §6 step 5)
	if next, err := s.issue(ctx, sub); err == nil {
		resp.NextChallenge = next
	} else {
		s.Logger.Warn("piggyback issue failed", "err", err)
	}
	return resp, nil
}

func (s *Server) ListAchievements(ctx context.Context, _ *buttonv1.ListAchievementsRequest) (*buttonv1.ListAchievementsResponse, error) {
	resp := &buttonv1.ListAchievementsResponse{}

	// personalization is opportunistic: only when a forwarded token parses
	// (anonymous rule — the header arrives verified when present, spec §4)
	unlocked := map[string]time.Time{}
	if sub, err := subFromContext(ctx); err == nil {
		rows, err := db.New(s.Pool).ListUserAchievements(ctx, sub)
		if err != nil {
			return nil, status.Error(codes.Unavailable, "postgres unavailable")
		}
		for _, r := range rows {
			unlocked[r.AchievementID] = r.UnlockedAt
		}
	}

	for _, a := range achievements.Catalog {
		pa := &buttonv1.Achievement{Id: a.ID, Title: a.Title, Description: a.Description}
		if at, ok := unlocked[a.ID]; ok {
			pa.UnlockedAt = timestamppb.New(at)
		}
		resp.Catalog = append(resp.Catalog, pa)
	}

	// reached milestones from Redis; Redis down → bare catalog still served
	keys := make([]string, len(achievements.Milestones))
	for i, m := range achievements.Milestones {
		keys[i] = fmt.Sprintf("milestone:%d", m.Threshold)
	}
	vals, err := s.RDB.MGet(ctx, keys...).Result()
	if err != nil {
		s.Logger.Warn("milestone read failed", "err", err)
		return resp, nil
	}
	for i, v := range vals {
		str, ok := v.(string)
		if !ok {
			continue // not reached
		}
		ts, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			continue
		}
		m := achievements.Milestones[i]
		resp.Milestones = append(resp.Milestones, &buttonv1.Milestone{
			Threshold: m.Threshold,
			Title:     m.Title,
			ReachedAt: timestamppb.New(time.Unix(ts, 0)),
		})
	}
	return resp, nil
}
