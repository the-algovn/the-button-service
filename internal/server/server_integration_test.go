//go:build integration

package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	buttonv1 "github.com/the-algovn/protos/gen/go/algovn/button/v1"
	"github.com/the-algovn/the-button-service/internal/countercache"
	"github.com/the-algovn/the-button-service/internal/difficulty"
	"github.com/the-algovn/the-button-service/internal/leaderboard"
	"github.com/the-algovn/the-button-service/internal/pow"
	"github.com/the-algovn/the-button-service/internal/publisher"
	"github.com/the-algovn/the-button-service/internal/store"
	"github.com/the-algovn/the-button-service/internal/testutil"
)

func TestEndToEnd_SubmitTickPublishCounter(t *testing.T) {
	pgURL := testutil.StartPostgres(t)
	redisURL := testutil.StartRedis(t)
	amqpURL := testutil.StartRabbit(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testutil.Migrate(t, pgURL)
	pool, err := store.NewPG(ctx, pgURL)
	require.NoError(t, err)
	defer pool.Close()
	rdb, err := store.NewRedis(ctx, redisURL)
	require.NoError(t, err)
	defer rdb.Close()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// real AMQP publisher + a queue bound to the counter channel
	conn, err := amqp.Dial(amqpURL)
	require.NoError(t, err)
	defer conn.Close()
	ch, err := conn.Channel()
	require.NoError(t, err)
	require.NoError(t, ch.ExchangeDeclare("events", "topic", true, false, false, false, nil))
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.QueueBind(q.Name, "the-button.counter", "events", false, nil))
	msgs, err := ch.Consume(q.Name, "", true, true, false, false, nil)
	require.NoError(t, err)
	lbQ, err := ch.QueueDeclare("", false, true, true, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.QueueBind(lbQ.Name, "the-button.leaderboard", "events", false, nil))
	lbMsgs, err := ch.Consume(lbQ.Name, "", true, true, false, false, nil)
	require.NoError(t, err)
	publish := func(channel string, body []byte) {
		_ = ch.PublishWithContext(ctx, "events", channel, false, false,
			amqp.Publishing{ContentType: "application/json", Body: body})
	}

	pub := &publisher.Publisher{Pool: pool, RDB: rdb, Publish: publish, Logger: logger}
	go pub.Run(ctx)
	cache := &countercache.Cache{RDB: rdb, Logger: logger}
	go cache.Run(ctx)
	diff := &difficulty.Cache{RDB: rdb, Logger: logger}
	go diff.Run(ctx)

	key := []byte("integration-test-key-0123456789a")
	srv := &Server{Pool: pool, RDB: rdb, Tick: cache, Diff: diff, Logger: logger, W0: 4, Keys: [][]byte{key}}

	// fails closed until the publisher writes pow:L / pow:min_interval
	require.Eventually(t, func() bool {
		_, err := srv.IssueChallenge(authCtx("user-1"), &buttonv1.IssueChallengeRequest{})
		return status.Code(err) != codes.Unavailable
	}, 15*time.Second, 200*time.Millisecond)

	chResp, err := srv.IssueChallenge(authCtx("user-1"), &buttonv1.IssueChallengeRequest{IntendedClicks: 25})
	require.NoError(t, err)
	require.EqualValues(t, pow.MaxBatch, chResp.MaxBatch)
	require.EqualValues(t, 4, chResp.WorkFactor)         // W0=4 × L=1
	require.EqualValues(t, 2, chResp.MinIntervalSeconds) // ladder at L=1

	// anonymous callers cannot get challenges
	_, err = srv.IssueChallenge(context.Background(), &buttonv1.IssueChallengeRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err))

	nonce := pow.Solve(chResp.Challenge, 4, 1, 25)
	sub, err := srv.SubmitClicks(authCtx("user-1"), &buttonv1.SubmitClicksRequest{
		Challenge: chResp.Challenge, Nonce: nonce, ClickCount: 25,
	})
	require.NoError(t, err)
	require.EqualValues(t, 25, sub.UserTotalClicks)
	require.NotNil(t, sub.NextChallenge, "next challenge must piggyback")

	// weekly bucket + profile are mirrored to Postgres asynchronously by the
	// persist worker (started by pub.Run) — poll rather than assert directly.
	require.Eventually(t, func() bool {
		var weekly int64
		err := pool.QueryRow(context.Background(),
			`SELECT clicks FROM user_weekly_clicks WHERE user_sub=$1 AND week_start=$2`,
			"user-1", leaderboard.WeekStart(time.Now())).Scan(&weekly)
		return err == nil && weekly == 25
	}, 15*time.Second, 200*time.Millisecond)

	require.Eventually(t, func() bool {
		var name string
		err := pool.QueryRow(context.Background(),
			`SELECT display_name FROM user_profile WHERE user_sub=$1`, "user-1").Scan(&name)
		return err == nil && name != ""
	}, 15*time.Second, 200*time.Millisecond)

	// bad work is rejected before touching state
	_, err = srv.SubmitClicks(authCtx("user-1"), &buttonv1.SubmitClicksRequest{
		Challenge: sub.NextChallenge.Challenge, Nonce: 0, ClickCount: 10_001,
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// another sub cannot spend user-1's token
	_, err = srv.SubmitClicks(authCtx("user-2"), &buttonv1.SubmitClicksRequest{
		Challenge: sub.NextChallenge.Challenge, Nonce: 0, ClickCount: 1,
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// the publisher broadcasts the new total on the counter channel
	waitFor := func(wantTotal uint64) {
		t.Helper()
		deadline := time.After(15 * time.Second)
		for {
			select {
			case m := <-msgs:
				var ev struct {
					Type  string `json:"type"`
					Total uint64 `json:"total"`
				}
				require.NoError(t, json.Unmarshal(m.Body, &ev))
				if ev.Type == "counter" && ev.Total == wantTotal {
					return
				}
			case <-deadline:
				t.Fatalf("no counter publish for total=%d observed", wantTotal)
			}
		}
	}
	waitFor(25)

	// GetCounter serves the per-replica cached total
	require.Eventually(t, func() bool {
		resp, err := srv.GetCounter(context.Background(), &buttonv1.GetCounterRequest{})
		return err == nil && resp.Total == 25
	}, 5*time.Second, 200*time.Millisecond)

	// ListAchievements: personalized for user-1, bare for anonymous. Both the
	// achievement unlock and user_total_clicks land via the async persist
	// worker, so poll until they show up.
	var la *buttonv1.ListAchievementsResponse
	require.Eventually(t, func() bool {
		var err error
		la, err = srv.ListAchievements(authCtx("user-1"), &buttonv1.ListAchievementsRequest{})
		if err != nil || la.UserTotalClicks != 25 {
			return false
		}
		for _, a := range la.Catalog {
			if a.Id == "mvh" && a.UnlockedAt != nil {
				return true
			}
		}
		return false
	}, 15*time.Second, 200*time.Millisecond)
	require.Len(t, la.Catalog, 12)

	// a signed-in user who has never clicked has no row: zero, not an error
	fresh, err := srv.ListAchievements(authCtx("user-never-clicked"), &buttonv1.ListAchievementsRequest{})
	require.NoError(t, err)
	require.Zero(t, fresh.UserTotalClicks)

	anon, err := srv.ListAchievements(context.Background(), &buttonv1.ListAchievementsRequest{})
	require.NoError(t, err)
	require.Len(t, anon.Catalog, 12)
	for _, a := range anon.Catalog {
		require.Nil(t, a.UnlockedAt)
	}
	require.Empty(t, anon.Milestones)     // total 25 — nothing reached
	require.Zero(t, anon.UserTotalClicks) // never personalized without a token

	// a leaderboard frame lands within a few publisher cycles and ranks user-1
	require.Eventually(t, func() bool {
		select {
		case m := <-lbMsgs:
			var f struct {
				Type    string `json:"type"`
				AllTime []struct {
					Name   string `json:"name"`
					Clicks uint64 `json:"clicks"`
				} `json:"allTime"`
			}
			if json.Unmarshal(m.Body, &f) != nil || f.Type != "leaderboard" {
				return false
			}
			return len(f.AllTime) >= 1 && f.AllTime[0].Clicks == 25
		default:
			return false
		}
	}, 10*time.Second, 200*time.Millisecond)

	// GetLeaderboard: anonymous sees the board; personalized sees own ranks
	lb, err := srv.GetLeaderboard(context.Background(), &buttonv1.GetLeaderboardRequest{})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(lb.AllTime), 1)
	require.Equal(t, uint64(25), lb.AllTime[0].Clicks)
	require.Zero(t, lb.MyAllTimeRank) // anonymous -> no personal rank

	me, err := srv.GetLeaderboard(authCtx("user-1"), &buttonv1.GetLeaderboardRequest{})
	require.NoError(t, err)
	require.Equal(t, uint32(1), me.MyAllTimeRank)
}
