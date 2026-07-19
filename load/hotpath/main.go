// Command hotpath drives the-button's NEW pure-ack hot path (Redis throttle
// SetNX + Kafka produce, i.e. clicks.Submit) at a fixed target rate against a
// local Redis + Redpanda, reporting per-submit ack latency percentiles and the
// achieved rate. This is the redesign's critical serving path — the old
// load/soak tool benchmarked the retired per-batch Postgres transaction, which
// the hot path no longer performs.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/the-algovn/the-button-service/internal/clicks"
	"github.com/the-algovn/the-button-service/internal/kafka"
	"github.com/the-algovn/the-button-service/internal/pow"
	"github.com/the-algovn/the-button-service/internal/store"
)

func main() {
	redisURL := flag.String("redis", "redis://127.0.0.1:6379", "redis URL")
	brokersCSV := flag.String("brokers", "127.0.0.1:19092", "comma-separated Kafka brokers")
	rate := flag.Int("rate", 2000, "target submits per second")
	dur := flag.Duration("duration", 30*time.Second, "run duration")
	users := flag.Int("users", 20000, "distinct user_sub values cycled")
	minInterval := flag.Uint("min-interval", 1, "per-user min interval seconds (throttle TTL)")
	batch := flag.Uint("batch", 100, "clicks per submit")
	flag.Parse()

	ctx := context.Background()
	rdb, err := store.NewRedis(ctx, *redisURL)
	if err != nil {
		log.Fatalf("redis: %v", err)
	}
	defer rdb.Close()

	brokers := []string{}
	for _, b := range splitTrim(*brokersCSV) {
		brokers = append(brokers, b)
	}
	prod, err := kafka.NewProducer(brokers)
	if err != nil {
		log.Fatalf("kafka producer: %v", err)
	}
	defer prod.Close()

	var (
		lat       []time.Duration
		latMu     sync.Mutex
		ok        int64
		throttled int64
		fail      int64
	)
	work := make(chan string, *rate)
	var wg sync.WaitGroup
	workers := *rate / 50
	if workers < 16 {
		workers = 16
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sub := range work {
				p := pow.Payload{ID: uuid.NewString(), Sub: sub, MinIntervalS: uint32(*minInterval)}
				t0 := time.Now()
				err := clicks.Submit(ctx, rdb, prod, p, uint32(*batch), time.Now(), "bench")
				d := time.Since(t0)
				switch {
				case err == nil:
					atomic.AddInt64(&ok, 1)
					latMu.Lock()
					lat = append(lat, d)
					latMu.Unlock()
				case status.Code(err) == codes.ResourceExhausted:
					atomic.AddInt64(&throttled, 1) // per-user throttle hit; not a produce failure
				default:
					atomic.AddInt64(&fail, 1)
				}
			}
		}()
	}

	tick := time.NewTicker(time.Second / time.Duration(*rate))
	defer tick.Stop()
	deadline := time.After(*dur)
	var dropped int64
loop:
	for {
		select {
		case <-deadline:
			break loop
		case <-tick.C:
			select {
			case work <- fmt.Sprintf("bench-%d", rand.Intn(*users)):
			default:
				dropped++ // producers saturated; records a shortfall
			}
		}
	}
	close(work)
	wg.Wait()

	latMu.Lock()
	sort.Slice(lat, func(a, b int) bool { return lat[a] < lat[b] })
	latMu.Unlock()

	fmt.Printf("target_rate=%d duration=%s batch=%d users=%d\n", *rate, *dur, *batch, *users)
	fmt.Printf("ok=%d throttled=%d fail=%d dropped=%d achieved_rate=%.0f/s\n",
		ok, throttled, fail, dropped, float64(ok)/dur.Seconds())
	if len(lat) > 0 {
		fmt.Printf("ack_latency p50=%s p95=%s p99=%s max=%s\n",
			pct(lat, 50), pct(lat, 95), pct(lat, 99), lat[len(lat)-1])
	}
	fmt.Printf("produced_clicks=%d (ok*batch — should match counter:total delta once the worker drains)\n", ok*int64(*batch))
	if fail > 0 {
		os.Exit(1)
	}
}

func splitTrim(s string) []string {
	var out []string
	cur := ""
	for _, r := range s + "," {
		if r == ',' {
			for len(cur) > 0 && cur[0] == ' ' {
				cur = cur[1:]
			}
			for len(cur) > 0 && cur[len(cur)-1] == ' ' {
				cur = cur[:len(cur)-1]
			}
			if cur != "" {
				out = append(out, cur)
			}
			cur = ""
			continue
		}
		cur += string(r)
	}
	return out
}

func pct(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := p * len(sorted) / 100
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}
