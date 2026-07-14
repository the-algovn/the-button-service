// Command sseclient is a small Go SSE load client for the-button's
// anonymous counter channel. Stock k6 cannot observe per-frame SSE timing
// (http.get blocks until the response completes, which an SSE stream never
// does), and building the xk6-sse extension adds a build dependency for
// marginal benefit here. A plain goroutine-per-connection Go client gives
// full control over connect pacing, per-frame gap measurement, and
// reconnect-latency tracking, with no non-stdlib dependencies.
//
// Two modes:
//   - ramp: progressively opens connections in stages, holds each stage,
//     and aborts the ramp on distress (connect failures, 5xx, frame-gap
//     blowout). Used for the SSE capacity ramp.
//   - hold: opens a fixed number of connections and holds them for a
//     duration, auto-reconnecting (with the same 0-5s jitter the SPA uses)
//     on drop. Used for the rollout drill, run alongside a kubectl rollout
//     restart in another terminal.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func main() {
	mode := flag.String("mode", "ramp", "ramp | hold")
	url := flag.String("url", "https://api.algovn.com/events/the-button.counter", "SSE URL")
	stagesFlag := flag.String("stages", "500,2000,5000,10000", "ramp: comma-separated VU targets")
	hold := flag.Duration("hold", 60*time.Second, "ramp: how long to hold each stage; hold-mode: total run duration")
	connectRate := flag.Float64("connect-rate", 12, "max new connections per second (stay under the 50/s, 1000/min per-IP limits)")
	statsInterval := flag.Duration("stats-interval", 5*time.Second, "how often to print a live status line")
	abortGapP95 := flag.Duration("abort-gap-p95", 3*time.Second, "ramp: abort if this stage's frame-gap p95 exceeds this")
	abortErrRate := flag.Float64("abort-err-rate", 0.02, "ramp: abort if connect failure rate exceeds this")
	jitterMax := flag.Duration("jitter-max", 5*time.Second, "hold: max random reconnect delay (matches SPA jitter)")
	vus := flag.Int("vus", 2000, "hold: number of connections to hold")
	out := flag.String("out", "", "optional path to append machine-readable summary lines")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var outF *os.File
	if *out != "" {
		f, err := os.OpenFile(*out, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			log.Fatalf("open -out: %v", err)
		}
		defer f.Close()
		outF = f
	}

	switch *mode {
	case "ramp":
		stages, err := parseStages(*stagesFlag)
		if err != nil {
			log.Fatalf("bad -stages: %v", err)
		}
		runRamp(ctx, *url, stages, *hold, *connectRate, *statsInterval, *abortGapP95, *abortErrRate, outF)
	case "hold":
		runHold(ctx, *url, *vus, *hold, *connectRate, *statsInterval, *jitterMax, outF)
	default:
		log.Fatalf("unknown -mode %q", *mode)
	}
}

func parseStages(s string) ([]int, error) {
	var out []int
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}

// ---- shared connection + metrics machinery ----

// metrics aggregates counters/gauges across all connection goroutines.
// Fields are updated with atomics from many goroutines; gapsMu guards the
// per-window gap sample slice, which is the only thing needing a lock.
type metrics struct {
	open            int64 // currently-open connections
	connectAttempts int64
	connectOK       int64
	connectFail     int64
	status5xx       int64
	status429       int64
	dataEvents      int64 // real "data:" frames (counter changed)
	heartbeats      int64 // ": ping" / other comment frames
	disconnects     int64
	reconnects      int64

	gapsMu sync.Mutex
	gapsMs []float64 // inter-frame gaps observed in the current window, all conns combined

	reconnMu sync.Mutex
	reconnMs []float64 // disconnect->reconnect latency samples in the current window

	errLogged int64 // number of connect-failure reasons logged so far (sampled, capped)
}

// maxErrSamples bounds how many connect-failure reasons we print, so a
// large-scale failure doesn't flood stdout with thousands of repeats.
const maxErrSamples = 20

func (m *metrics) resetWindow() (gaps, reconn []float64) {
	m.gapsMu.Lock()
	gaps = m.gapsMs
	m.gapsMs = nil
	m.gapsMu.Unlock()
	m.reconnMu.Lock()
	reconn = m.reconnMs
	m.reconnMs = nil
	m.reconnMu.Unlock()
	return gaps, reconn
}

func percentiles(xs []float64) (p50, p95, max float64) {
	if len(xs) == 0 {
		return 0, 0, 0
	}
	cp := append([]float64(nil), xs...)
	sort.Float64s(cp)
	p := func(q float64) float64 {
		i := int(q * float64(len(cp)-1))
		return cp[i]
	}
	return p(0.50), p(0.95), cp[len(cp)-1]
}

// runOneConn opens the SSE stream and blocks until it errors, EOF's, or ctx
// is cancelled. It reports connect/disconnect/frame events on m. It does
// NOT reconnect itself — callers (ramp vs hold) decide reconnect policy.
// It returns true if the stream reached the open state (200 + streaming),
// regardless of how it later ended — callers use this to drive a
// SPA-equivalent reconnect backoff (see reconnectDelay).
func runOneConn(ctx context.Context, client *http.Client, url string, m *metrics) bool {
	atomic.AddInt64(&m.connectAttempts, 1)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		atomic.AddInt64(&m.connectFail, 1)
		return false
	}
	req.Header.Set("Accept", "text/event-stream")

	resp, err := client.Do(req)
	if err != nil {
		atomic.AddInt64(&m.connectFail, 1)
		if n := atomic.AddInt64(&m.errLogged, 1); n <= maxErrSamples {
			log.Printf("connect error (sample %d/%d): %v", n, maxErrSamples, err)
		}
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		atomic.AddInt64(&m.connectFail, 1)
		if resp.StatusCode == 429 {
			atomic.AddInt64(&m.status429, 1)
		} else if resp.StatusCode >= 500 {
			atomic.AddInt64(&m.status5xx, 1)
		}
		if n := atomic.AddInt64(&m.errLogged, 1); n <= maxErrSamples {
			log.Printf("connect non-200 (sample %d/%d): status=%d", n, maxErrSamples, resp.StatusCode)
		}
		return false
	}

	atomic.AddInt64(&m.connectOK, 1)
	atomic.AddInt64(&m.open, 1)
	defer atomic.AddInt64(&m.open, -1)

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 4096), 1<<20)

	var lastFrame time.Time
	sawData, sawAny := false, false
	flush := func() {
		if !sawAny {
			return
		}
		now := time.Now()
		if !lastFrame.IsZero() {
			gap := now.Sub(lastFrame).Seconds() * 1000
			m.gapsMu.Lock()
			m.gapsMs = append(m.gapsMs, gap)
			m.gapsMu.Unlock()
		}
		lastFrame = now
		if sawData {
			atomic.AddInt64(&m.dataEvents, 1)
		} else {
			atomic.AddInt64(&m.heartbeats, 1)
		}
		sawData, sawAny = false, false
	}
	for sc.Scan() {
		select {
		case <-ctx.Done():
			atomic.AddInt64(&m.disconnects, 1)
			return true
		default:
		}
		line := sc.Text()
		if line == "" {
			flush()
			continue
		}
		sawAny = true
		if strings.HasPrefix(line, "data:") {
			sawData = true
		}
	}
	atomic.AddInt64(&m.disconnects, 1)
	return true
}

func newHTTPClient() *http.Client {
	return &http.Client{
		// No overall Timeout: these are intentionally long-lived streams.
		// Transport otherwise uses stdlib defaults (h2 auto-negotiated,
		// unlimited conns per host, multiplexing many streams per conn).
	}
}

// pacer yields once per connection slot at connectsPerSec, honoring ctx.
type pacer struct {
	interval time.Duration
	last     time.Time
	mu       sync.Mutex
}

func newPacer(perSec float64) *pacer {
	if perSec <= 0 {
		perSec = 1
	}
	return &pacer{interval: time.Duration(float64(time.Second) / perSec)}
}

func (p *pacer) wait(ctx context.Context) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	next := p.last.Add(p.interval)
	if next.After(now) {
		t := time.NewTimer(next.Sub(now))
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
			return false
		}
		now = time.Now()
	}
	p.last = now
	return true
}

func logStatus(w *os.File, msg string) {
	line := fmt.Sprintf("[%s] %s", time.Now().Format(time.RFC3339), msg)
	fmt.Println(line)
	if w != nil {
		fmt.Fprintln(w, line)
	}
}

// ---- ramp mode ----

func runRamp(ctx context.Context, url string, stages []int, hold time.Duration, connectRate float64,
	statsInterval, abortGapP95 time.Duration, abortErrRate float64, out *os.File) {

	client := newHTTPClient()
	m := &metrics{}
	pc := newPacer(connectRate)
	var conns []context.CancelFunc
	var connsMu sync.Mutex

	openTo := func(ctx context.Context, target int) bool {
		current := len(conns)
		for current < target {
			if !pc.wait(ctx) {
				return false
			}
			cctx, cancel := context.WithCancel(ctx)
			connsMu.Lock()
			conns = append(conns, cancel)
			connsMu.Unlock()
			go runOneConn(cctx, client, url, m)
			current++
		}
		return true
	}

	closeAll := func() {
		connsMu.Lock()
		defer connsMu.Unlock()
		for _, c := range conns {
			c()
		}
	}
	defer closeAll()

	statusTicker := time.NewTicker(statsInterval)
	defer statusTicker.Stop()
	stopStatus := make(chan struct{})
	go func() {
		for {
			select {
			case <-statusTicker.C:
				logStatus(out, fmt.Sprintf(
					"live open=%d attempts=%d ok=%d fail=%d 5xx=%d 429=%d dataEvents=%d heartbeats=%d",
					atomic.LoadInt64(&m.open), atomic.LoadInt64(&m.connectAttempts), atomic.LoadInt64(&m.connectOK),
					atomic.LoadInt64(&m.connectFail), atomic.LoadInt64(&m.status5xx), atomic.LoadInt64(&m.status429),
					atomic.LoadInt64(&m.dataEvents), atomic.LoadInt64(&m.heartbeats)))
			case <-stopStatus:
				return
			}
		}
	}()
	defer close(stopStatus)

	for _, target := range stages {
		logStatus(out, fmt.Sprintf("=== stage start: ramping to %d (connect-rate=%.1f/s) ===", target, connectRate))
		attemptsBefore := atomic.LoadInt64(&m.connectAttempts)
		failBefore := atomic.LoadInt64(&m.connectFail)
		if !openTo(ctx, target) {
			logStatus(out, "ramp interrupted during connect phase")
			return
		}
		logStatus(out, fmt.Sprintf("=== stage %d: reached open=%d, holding %s ===", target, atomic.LoadInt64(&m.open), hold))
		m.resetWindow() // drop pre-stage samples so the stage's gap stats are clean

		holdDeadline := time.After(hold)
	holdLoop:
		for {
			select {
			case <-holdDeadline:
				break holdLoop
			case <-ctx.Done():
				logStatus(out, "ramp interrupted during hold")
				return
			case <-time.After(1 * time.Second):
			}
		}

		gaps, _ := m.resetWindow()
		p50, p95, max := percentiles(gaps)
		attempts := atomic.LoadInt64(&m.connectAttempts) - attemptsBefore
		fails := atomic.LoadInt64(&m.connectFail) - failBefore
		var errRate float64
		if attempts > 0 {
			errRate = float64(fails) / float64(attempts)
		}
		logStatus(out, fmt.Sprintf(
			"=== stage %d RESULT: open=%d frame-gap(ms) p50=%.0f p95=%.0f max=%.0f samples=%d | stage-attempts=%d stage-fails=%d errRate=%.4f 5xx=%d 429=%d ===",
			target, atomic.LoadInt64(&m.open), p50, p95, max, len(gaps), attempts, fails, errRate,
			atomic.LoadInt64(&m.status5xx), atomic.LoadInt64(&m.status429)))

		abort := false
		var reason string
		if atomic.LoadInt64(&m.status5xx) > 0 {
			abort, reason = true, "5xx responses observed"
		} else if errRate > abortErrRate {
			abort, reason = true, fmt.Sprintf("connect failure rate %.2f%% > %.2f%%", errRate*100, abortErrRate*100)
		} else if p95 > float64(abortGapP95.Milliseconds()) && len(gaps) > 10 {
			abort, reason = true, fmt.Sprintf("frame-gap p95 %.0fms > %v", p95, abortGapP95)
		} else if atomic.LoadInt64(&m.open) < int64(float64(target)*0.98) {
			abort, reason = true, fmt.Sprintf("open count %d fell short of target %d (>2%% short)", atomic.LoadInt64(&m.open), target)
		}
		if abort {
			logStatus(out, fmt.Sprintf("*** ABORT: %s — stopping ramp at stage target=%d ***", reason, target))
			return
		}
	}
	logStatus(out, fmt.Sprintf("=== ramp completed all stages without hitting an abort condition; final open=%d ===", atomic.LoadInt64(&m.open)))
}

// ---- hold mode (rollout drill) ----

// reconnectCapMaxMs mirrors RECONNECT_CAP_MAX_MS in the SPA's
// web/apps/the-button/src/lib/liveCounter.ts.
const reconnectCapMaxMs = 60_000.0

// reconnectDelay mirrors the SPA's full-jitter backoff: the cap doubles per
// consecutive connect failure starting at startCap (RECONNECT_CAP_START_MS
// there, default 5s here via -jitter-max), capped at 60s; the actual delay
// is uniform in [0, cap). failures resets to 1 on every successful open
// (matching the SPA's onopen handler), so a stream that opens and later
// drops reconnects with the same short cap as a first attempt — only
// consecutive FAILURES TO OPEN (e.g. getting rate-limited) grow the cap,
// which is what lets a reconnect storm against a per-IP rate limit
// de-synchronize instead of self-sustaining.
func reconnectDelay(startCap time.Duration, failures int) time.Duration {
	if failures < 1 {
		failures = 1
	}
	capMs := float64(startCap.Milliseconds()) * math.Pow(2, float64(failures-1))
	if capMs > reconnectCapMaxMs {
		capMs = reconnectCapMaxMs
	}
	return time.Duration(rand.Float64() * capMs * float64(time.Millisecond))
}

func runHold(ctx context.Context, url string, vus int, duration time.Duration, connectRate float64,
	statsInterval time.Duration, jitterMax time.Duration, out *os.File) {

	client := newHTTPClient()
	m := &metrics{}
	pc := newPacer(connectRate)

	runCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		if !pc.wait(runCtx) {
			return
		}
		failures := 0
		for {
			select {
			case <-runCtx.Done():
				return
			default:
			}
			opened := runOneConn(runCtx, client, url, m)
			// runOneConn returned: either ctx done, or the stream dropped.
			select {
			case <-runCtx.Done():
				return
			default:
			}
			downSince := time.Now()
			if opened {
				failures = 1
			} else {
				failures++
			}
			t := time.NewTimer(reconnectDelay(jitterMax, failures))
			select {
			case <-t.C:
			case <-runCtx.Done():
				t.Stop()
				return
			}
			reconnectAt := time.Now()
			latency := reconnectAt.Sub(downSince).Seconds() * 1000
			m.reconnMu.Lock()
			m.reconnMs = append(m.reconnMs, latency)
			m.reconnMu.Unlock()
			atomic.AddInt64(&m.reconnects, 1)
		}
	}
	logStatus(out, fmt.Sprintf("=== hold: opening %d connections (connect-rate=%.1f/s), holding %s ===", vus, connectRate, duration))
	for i := 0; i < vus; i++ {
		wg.Add(1)
		go worker()
	}

	statusTicker := time.NewTicker(statsInterval)
	defer statusTicker.Stop()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	for {
		select {
		case <-statusTicker.C:
			gaps, reconn := m.resetWindow()
			p50, p95, max := percentiles(gaps)
			rp50, rp95, rmax := percentiles(reconn)
			logStatus(out, fmt.Sprintf(
				"live open=%d attempts=%d ok=%d fail=%d disconnects=%d reconnects=%d | frame-gap(ms) p50=%.0f p95=%.0f max=%.0f n=%d | reconnect-latency(ms) p50=%.0f p95=%.0f max=%.0f n=%d | 5xx=%d 429=%d",
				atomic.LoadInt64(&m.open), atomic.LoadInt64(&m.connectAttempts), atomic.LoadInt64(&m.connectOK), atomic.LoadInt64(&m.connectFail),
				atomic.LoadInt64(&m.disconnects), atomic.LoadInt64(&m.reconnects),
				p50, p95, max, len(gaps), rp50, rp95, rmax, len(reconn),
				atomic.LoadInt64(&m.status5xx), atomic.LoadInt64(&m.status429)))
		case <-done:
			logStatus(out, "=== hold complete ===")
			return
		case <-ctx.Done():
			logStatus(out, "=== hold interrupted ===")
			<-done
			return
		}
	}
}
