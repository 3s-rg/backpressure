package backpressure_test

import (
	"context"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	bp "github.com/stano45/backpressure/go"
	"github.com/valkey-io/valkey-go"
)

// These tests target a real Valkey on localhost:6379 (the repo's
// docker-compose.yml binding). If Valkey is unreachable they are skipped.

func valkeyAddr() string {
	if v := os.Getenv("BACKPRESSURE_TEST_VALKEY"); v != "" {
		return v
	}
	return "127.0.0.1:6379"
}

func newTestClient(tb testing.TB) valkey.Client {
	tb.Helper()
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress:  []string{valkeyAddr()},
		DisableCache: true,
	})
	if err != nil {
		tb.Skipf("valkey unreachable at %s: %v", valkeyAddr(), err)
	}
	// Sanity ping with a short timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Do(ctx, client.B().Ping().Build()).Error(); err != nil {
		client.Close()
		tb.Skipf("valkey ping failed at %s: %v", valkeyAddr(), err)
	}
	tb.Cleanup(client.Close)
	return client
}

// uniqueTargetID isolates each test from the others, the admission script
// writes to bpq:{target}:tb:* keys that we don't share.
func uniqueTargetID(tb testing.TB) string {
	tb.Helper()
	return "gotest:" + tb.Name() + ":" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

func cleanupTarget(tb testing.TB, c valkey.Client, target bp.TargetConfig) {
	tb.Helper()
	ctx := context.Background()
	keys := []string{
		bp.TBGlobalKey(target.TargetID),
		bp.TBPriKey(target.TargetID),
		bp.TBTPKey(target.TargetID),
		bp.TBRegKey(target.TargetID),
	}
	// Also delete the per-dimension multi-dim keys. For single-dim targets
	// these never exist, so the extra Del entries are harmless no-ops.
	for _, con := range target.Constraints {
		keys = append(keys,
			bp.TBGlobalKeyDim(target.TargetID, con.Dimension),
			bp.TBPriKeyDim(target.TargetID, con.Dimension),
		)
	}
	_ = c.Do(ctx, c.B().Del().Key(keys...).Build()).Error()
}

func makeTarget(tb testing.TB, opts ...func(*bp.TargetConfig)) bp.TargetConfig {
	tb.Helper()
	tc := bp.TargetConfig{
		TargetID:    uniqueTargetID(tb),
		Constraints: []bp.Constraint{{Dimension: "rps", Limit: 100}},
	}
	for _, opt := range opts {
		opt(&tc)
	}
	return tc
}

// --- TestUnderCapGrants -----------------------

func TestUnderCapGrants(t *testing.T) {
	c := newTestClient(t)
	target := makeTarget(t, func(tc *bp.TargetConfig) {
		tc.TenantWeights = map[string]float64{"a": 1.0}
		tc.PriorityMinimums = map[int]float64{1: 1.0, 2: 0.0, 3: 0.0}
	})
	cleanupTarget(t, c, target)
	defer cleanupTarget(t, c, target)

	stats, err := bp.QueuedAcquire(context.Background(), c, target, bp.QueuedAcquireRequest{
		TenantID:      "a",
		Priority:      1,
		ResourceCosts: map[string]float64{"rps": 1.0},
		RequestID:     "x",
		TimeoutS:      5.0,
		Blocking:      true,
	}, nil)
	if err != nil {
		t.Fatalf("acquire returned err: %v", err)
	}
	if stats.Polls < 1 {
		t.Errorf("polls=%d, want >=1", stats.Polls)
	}
	if stats.GrantedAtMs == 0 {
		t.Errorf("granted_at_ms=%d, want >0", stats.GrantedAtMs)
	}
}

// --- TestOverCapDeniesNonBlocking --

func TestOverCapDeniesNonBlocking(t *testing.T) {
	t.Setenv("QUEUED_GATE_BURST_SECONDS", "0.01")
	c := newTestClient(t)
	target := makeTarget(t, func(tc *bp.TargetConfig) {
		tc.TenantWeights = map[string]float64{"a": 1.0}
		tc.PriorityMinimums = map[int]float64{1: 1.0, 2: 0.0, 3: 0.0}
	})
	cleanupTarget(t, c, target)
	defer cleanupTarget(t, c, target)

	// Drain global capacity with one big grant.
	if _, err := bp.QueuedAcquire(context.Background(), c, target, bp.QueuedAcquireRequest{
		TenantID:      "a",
		Priority:      1,
		ResourceCosts: map[string]float64{"rps": 1.0},
		RequestID:     "x1",
		TimeoutS:      5.0,
		Blocking:      true,
	}, nil); err != nil {
		t.Fatalf("warm-up acquire: %v", err)
	}

	// Now ask for far more than is available, non-blocking -> DeniedError.
	_, err := bp.QueuedAcquire(context.Background(), c, target, bp.QueuedAcquireRequest{
		TenantID:      "a",
		Priority:      1,
		ResourceCosts: map[string]float64{"rps": 100.0},
		RequestID:     "x2",
		TimeoutS:      5.0,
		Blocking:      false,
	}, nil)
	retry, ok := bp.IsDenied(err)
	if !ok {
		t.Fatalf("expected DeniedError, got %v", err)
	}
	if retry < 1 {
		t.Errorf("retry_after_ms=%d, want >=1", retry)
	}
}

// --- TestRefillAllowsSubsequentGrant ---------------------------------------

func TestRefillAllowsSubsequentGrant(t *testing.T) {
	t.Setenv("QUEUED_GATE_BURST_SECONDS", "0.05")
	c := newTestClient(t)
	target := makeTarget(t, func(tc *bp.TargetConfig) {
		tc.TenantWeights = map[string]float64{"a": 1.0}
		tc.PriorityMinimums = map[int]float64{1: 1.0, 2: 0.0, 3: 0.0}
	})
	cleanupTarget(t, c, target)
	defer cleanupTarget(t, c, target)

	if _, err := bp.QueuedAcquire(context.Background(), c, target, bp.QueuedAcquireRequest{
		TenantID:      "a",
		Priority:      1,
		ResourceCosts: map[string]float64{"rps": 4.0},
		RequestID:     "x1",
		TimeoutS:      5.0,
		Blocking:      true,
	}, nil); err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	time.Sleep(60 * time.Millisecond)

	if _, err := bp.QueuedAcquire(context.Background(), c, target, bp.QueuedAcquireRequest{
		TenantID:      "a",
		Priority:      1,
		ResourceCosts: map[string]float64{"rps": 2.0},
		RequestID:     "x2",
		TimeoutS:      5.0,
		Blocking:      false,
	}, nil); err != nil {
		t.Fatalf("after refill, expected grant, got %v", err)
	}
}

// --- TestOversizedRequestDeniedWhenGlobalEmpty -----------------------------

func TestOversizedRequestDeniedWhenGlobalEmpty(t *testing.T) {
	c := newTestClient(t)
	target := makeTarget(t, func(tc *bp.TargetConfig) {
		tc.TenantWeights = map[string]float64{"a": 1.0, "b": 1.0}
		tc.PriorityMinimums = map[int]float64{1: 0.6, 2: 0.2, 3: 0.2}
	})
	cleanupTarget(t, c, target)
	defer cleanupTarget(t, c, target)

	for i := 0; i < 50; i++ {
		if _, err := bp.QueuedAcquire(context.Background(), c, target, bp.QueuedAcquireRequest{
			TenantID:      "a",
			Priority:      1,
			ResourceCosts: map[string]float64{"rps": 1.0},
			RequestID:     "p1-" + strconv.Itoa(i),
			TimeoutS:      30.0,
			Blocking:      true,
		}, nil); err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
	}

	_, err := bp.QueuedAcquire(context.Background(), c, target, bp.QueuedAcquireRequest{
		TenantID:      "b",
		Priority:      3,
		ResourceCosts: map[string]float64{"rps": 500.0},
		RequestID:     "p3-big",
		TimeoutS:      1.0,
		Blocking:      false,
	}, nil)
	if _, ok := bp.IsDenied(err); !ok {
		t.Fatalf("expected DeniedError, got %v", err)
	}
}

// --- TestTenantFairnessHeavyAndLight ---------------------------------------

func TestTenantFairnessHeavyAndLight(t *testing.T) {
	c := newTestClient(t)
	target := makeTarget(t, func(tc *bp.TargetConfig) {
		tc.TenantWeights = map[string]float64{"heavy": 3.0, "light": 1.0}
	})
	cleanupTarget(t, c, target)
	defer cleanupTarget(t, c, target)

	for i := 0; i < 5; i++ {
		if _, err := bp.QueuedAcquire(context.Background(), c, target, bp.QueuedAcquireRequest{
			TenantID:      "heavy",
			Priority:      1,
			ResourceCosts: map[string]float64{"rps": 1.0},
			RequestID:     "h-" + strconv.Itoa(i),
			TimeoutS:      10.0,
			Blocking:      true,
		}, nil); err != nil {
			t.Fatalf("heavy iter %d: %v", i, err)
		}
		if _, err := bp.QueuedAcquire(context.Background(), c, target, bp.QueuedAcquireRequest{
			TenantID:      "light",
			Priority:      1,
			ResourceCosts: map[string]float64{"rps": 1.0},
			RequestID:     "l-" + strconv.Itoa(i),
			TimeoutS:      10.0,
			Blocking:      true,
		}, nil); err != nil {
			t.Fatalf("light iter %d: %v", i, err)
		}
	}
}

// --- TestTPSweepRemovesStaleBucket -----------------------------------------

func TestTPSweepRemovesStaleBucket(t *testing.T) {
	t.Setenv("QUEUED_GATE_REGISTRY_IDLE_MS", "500")
	c := newTestClient(t)
	target := makeTarget(t, func(tc *bp.TargetConfig) {
		tc.TenantWeights = map[string]float64{"a": 1.0}
	})
	cleanupTarget(t, c, target)
	defer cleanupTarget(t, c, target)

	ctx := context.Background()
	tpKey := bp.TBTPKey(target.TargetID)
	if err := c.Do(ctx, c.B().Hset().Key(tpKey).FieldValue().FieldValue("ghost:1", "0:1").Build()).Error(); err != nil {
		t.Fatalf("seed ghost: %v", err)
	}
	if _, err := bp.QueuedAcquire(ctx, c, target, bp.QueuedAcquireRequest{
		TenantID:      "a",
		Priority:      1,
		ResourceCosts: map[string]float64{"rps": 1.0},
		RequestID:     "sweep-1",
		TimeoutS:      5.0,
		Blocking:      true,
	}, nil); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	got := c.Do(ctx, c.B().Hget().Key(tpKey).Field("ghost:1").Build())
	if err := got.Error(); err != nil && !valkey.IsValkeyNil(err) {
		t.Fatalf("hget ghost: %v", err)
	}
	if s, _ := got.ToString(); s != "" {
		t.Errorf("ghost field still present: %q", s)
	}
}

// --- TestReleaseRestoresCapacity -------------------------------------------

func TestReleaseRestoresCapacity(t *testing.T) {
	c := newTestClient(t)
	target := makeTarget(t, func(tc *bp.TargetConfig) {
		tc.TenantWeights = map[string]float64{"a": 1.0}
	})
	cleanupTarget(t, c, target)
	defer cleanupTarget(t, c, target)

	ctx := context.Background()
	if _, err := bp.QueuedAcquire(ctx, c, target, bp.QueuedAcquireRequest{
		TenantID:      "a",
		Priority:      2,
		ResourceCosts: map[string]float64{"rps": 10.0},
		RequestID:     "acq",
		TimeoutS:      10.0,
		Blocking:      true,
	}, nil); err != nil {
		t.Fatalf("acquire: %v", err)
	}

	beforeStr, err := c.Do(ctx, c.B().Hget().Key(bp.TBGlobalKey(target.TargetID)).Field("budget").Build()).ToString()
	if err != nil {
		t.Fatalf("hget budget pre-release: %v", err)
	}
	before, _ := strconv.ParseFloat(beforeStr, 64)

	if err := bp.QueuedRelease(ctx, c, target, "a", map[string]float64{"rps": 10.0}, 2); err != nil {
		t.Fatalf("release: %v", err)
	}

	afterStr, err := c.Do(ctx, c.B().Hget().Key(bp.TBGlobalKey(target.TargetID)).Field("budget").Build()).ToString()
	if err != nil {
		t.Fatalf("hget budget post-release: %v", err)
	}
	after, _ := strconv.ParseFloat(afterStr, 64)

	if after < before {
		t.Errorf("budget shrank after release: before=%v after=%v", before, after)
	}
}

// --- TestBlockingAcquireRespectsCtxCancel ----------------------------------

func TestBlockingAcquireRespectsCtxCancel(t *testing.T) {
	c := newTestClient(t)
	// limit=10, default burst=1s, mult=1.2 -> global cap ~12.
	// Drain with cost=12 fills the bucket exactly, the follow-up needs ~500ms
	// of refill before it could succeed.
	target := makeTarget(t, func(tc *bp.TargetConfig) {
		tc.Constraints = []bp.Constraint{{Dimension: "rps", Limit: 10}}
		tc.TenantWeights = map[string]float64{"a": 1.0}
		tc.PriorityMinimums = map[int]float64{1: 1.0, 2: 0.0, 3: 0.0}
	})
	cleanupTarget(t, c, target)
	defer cleanupTarget(t, c, target)

	if _, err := bp.QueuedAcquire(context.Background(), c, target, bp.QueuedAcquireRequest{
		TenantID:      "a",
		Priority:      1,
		ResourceCosts: map[string]float64{"rps": 12.0},
		RequestID:     "drain",
		TimeoutS:      5.0,
		Blocking:      true,
	}, nil); err != nil {
		t.Fatalf("drain: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_, err := bp.QueuedAcquire(ctx, c, target, bp.QueuedAcquireRequest{
		TenantID:      "a",
		Priority:      1,
		ResourceCosts: map[string]float64{"rps": 5.0},
		RequestID:     "wait",
		TimeoutS:      30.0,
		Blocking:      true,
	}, nil)
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}

// --- TestTimeoutErrorOnLongDeny --------------------------------------------

func TestTimeoutErrorOnLongDeny(t *testing.T) {
	c := newTestClient(t)
	target := makeTarget(t, func(tc *bp.TargetConfig) {
		tc.Constraints = []bp.Constraint{{Dimension: "rps", Limit: 10}}
		tc.TenantWeights = map[string]float64{"a": 1.0}
		tc.PriorityMinimums = map[int]float64{1: 1.0, 2: 0.0, 3: 0.0}
	})
	cleanupTarget(t, c, target)
	defer cleanupTarget(t, c, target)

	if _, err := bp.QueuedAcquire(context.Background(), c, target, bp.QueuedAcquireRequest{
		TenantID:      "a",
		Priority:      1,
		ResourceCosts: map[string]float64{"rps": 12.0},
		RequestID:     "drain",
		TimeoutS:      5.0,
		Blocking:      true,
	}, nil); err != nil {
		t.Fatalf("drain: %v", err)
	}

	_, err := bp.QueuedAcquire(context.Background(), c, target, bp.QueuedAcquireRequest{
		TenantID:      "a",
		Priority:      1,
		ResourceCosts: map[string]float64{"rps": 5.0},
		RequestID:     "wait",
		TimeoutS:      0.05,
		Blocking:      true,
	}, nil)
	if !bp.IsTimeout(err) {
		t.Fatalf("expected TimeoutError, got %v", err)
	}
}
