package backpressure_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bp "github.com/stano45/backpressure/go"
)

// These tests mirror core/tests/test_multi_gate.py so the Go multi-dimension
// (N>=2) DRF admission path can be compared head-to-head with Python. They
// target a real Valkey on localhost:6379 and are skipped if it is unreachable.

func floatPtr(f float64) *float64 { return &f }

// makeMultiTarget builds the two-dimension (rpm + tpm) target used by
// test_multi_gate.py's _two_dim_target.
func makeMultiTarget(tb testing.TB, opts ...func(*bp.TargetConfig)) bp.TargetConfig {
	tb.Helper()
	tc := bp.TargetConfig{
		TargetID: uniqueTargetID(tb),
		Constraints: []bp.Constraint{
			{Dimension: "rpm", Limit: 60.0, DefaultCost: floatPtr(1.0)},
			{Dimension: "tpm", Limit: 12_000.0, DefaultCost: floatPtr(200.0)},
		},
	}
	for _, opt := range opts {
		opt(&tc)
	}
	return tc
}

// --- TestMultiUnderCapGrants ------------------

func TestMultiUnderCapGrants(t *testing.T) {
	c := newTestClient(t)
	target := makeMultiTarget(t, func(tc *bp.TargetConfig) {
		tc.TenantWeights = map[string]float64{"a": 1.0}
		tc.PriorityMinimums = map[int]float64{1: 1.0, 2: 0.0, 3: 0.0}
	})
	cleanupTarget(t, c, target)
	defer cleanupTarget(t, c, target)

	stats, err := bp.QueuedAcquire(context.Background(), c, target, bp.QueuedAcquireRequest{
		TenantID:      "a",
		Priority:      1,
		ResourceCosts: map[string]float64{"rpm": 1.0, "tpm": 100.0},
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
	if stats.GrantedAtMs <= 0 {
		t.Errorf("granted_at_ms=%d, want >0", stats.GrantedAtMs)
	}
	// rpm share = 1/60 = 0.0167 dominates tpm share = 100/12000 = 0.0083, so
	// the gate reports rpm as the dominant dimension on grant.
	if stats.DominantDim != "rpm" {
		t.Errorf("DominantDim=%q, want %q", stats.DominantDim, "rpm")
	}
}

// --- TestMultiTPMDominantDenies ----------------------------------------------

func TestMultiTPMDominantDenies(t *testing.T) {
	// Tight burst so the first request exhausts capacity quickly.
	t.Setenv("QUEUED_GATE_BURST_SECONDS", "0.05")
	c := newTestClient(t)
	target := makeMultiTarget(t, func(tc *bp.TargetConfig) {
		tc.TenantWeights = map[string]float64{"a": 1.0, "b": 1.0}
		tc.PriorityMinimums = map[int]float64{1: 1.0, 2: 0.0, 3: 0.0}
	})
	cleanupTarget(t, c, target)
	defer cleanupTarget(t, c, target)

	if _, err := bp.QueuedAcquire(context.Background(), c, target, bp.QueuedAcquireRequest{
		TenantID:      "a",
		Priority:      1,
		ResourceCosts: map[string]float64{"rpm": 1.0, "tpm": 6_000.0},
		RequestID:     "x1",
		TimeoutS:      5.0,
		Blocking:      true,
	}, nil); err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	_, err := bp.QueuedAcquire(context.Background(), c, target, bp.QueuedAcquireRequest{
		TenantID:      "a",
		Priority:      1,
		ResourceCosts: map[string]float64{"rpm": 1.0, "tpm": 6_000.0},
		RequestID:     "x2",
		TimeoutS:      5.0,
		Blocking:      false,
	}, nil)

	var de *bp.DeniedError
	if !errors.As(err, &de) {
		t.Fatalf("expected *DeniedError, got %v", err)
	}
	if de.RetryAfterMs < 1 {
		t.Errorf("retry_after_ms=%d, want >=1", de.RetryAfterMs)
	}
	// Multi-dim denies carry the binding label (e.g. "tpm:global" | "share").
	if de.BindingLabel == "" {
		t.Errorf("expected non-empty BindingLabel on multi-dim deny")
	}
}

// --- TestMultiReleaseRefundsAllDims ---

func TestMultiReleaseRefundsAllDims(t *testing.T) {
	t.Setenv("QUEUED_GATE_BURST_SECONDS", "0.05")
	c := newTestClient(t)
	target := makeMultiTarget(t, func(tc *bp.TargetConfig) {
		tc.TenantWeights = map[string]float64{"a": 1.0}
		tc.PriorityMinimums = map[int]float64{1: 1.0, 2: 0.0, 3: 0.0}
	})
	cleanupTarget(t, c, target)
	defer cleanupTarget(t, c, target)

	ctx := context.Background()
	costs := map[string]float64{"rpm": 1.0, "tpm": 6_000.0}
	if _, err := bp.QueuedAcquire(ctx, c, target, bp.QueuedAcquireRequest{
		TenantID:      "a",
		Priority:      1,
		ResourceCosts: costs,
		RequestID:     "x1",
		TimeoutS:      5.0,
		Blocking:      true,
	}, nil); err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	if err := bp.QueuedRelease(ctx, c, target, "a", costs, 1); err != nil {
		t.Fatalf("release: %v", err)
	}

	if _, err := bp.QueuedAcquire(ctx, c, target, bp.QueuedAcquireRequest{
		TenantID:      "a",
		Priority:      1,
		ResourceCosts: costs,
		RequestID:     "x2",
		TimeoutS:      5.0,
		Blocking:      true,
	}, nil); err != nil {
		t.Fatalf("second acquire after release: %v", err)
	}
}

// --- TestMultiRefillAllowsSubsequentGrant ------------------------------------

func TestMultiRefillAllowsSubsequentGrant(t *testing.T) {
	t.Setenv("QUEUED_GATE_BURST_SECONDS", "0.05")
	c := newTestClient(t)
	target := makeMultiTarget(t, func(tc *bp.TargetConfig) {
		tc.TenantWeights = map[string]float64{"a": 1.0}
		tc.PriorityMinimums = map[int]float64{1: 1.0, 2: 0.0, 3: 0.0}
	})
	cleanupTarget(t, c, target)
	defer cleanupTarget(t, c, target)

	ctx := context.Background()
	costs := map[string]float64{"rpm": 1.0, "tpm": 6_000.0}
	if _, err := bp.QueuedAcquire(ctx, c, target, bp.QueuedAcquireRequest{
		TenantID:      "a",
		Priority:      1,
		ResourceCosts: costs,
		RequestID:     "x1",
		TimeoutS:      5.0,
		Blocking:      true,
	}, nil); err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	stats, err := bp.QueuedAcquire(ctx, c, target, bp.QueuedAcquireRequest{
		TenantID:      "a",
		Priority:      1,
		ResourceCosts: costs,
		RequestID:     "x2",
		TimeoutS:      5.0,
		Blocking:      true,
	}, nil)
	if err != nil {
		t.Fatalf("blocking acquire after refill: %v", err)
	}
	if stats.Polls < 1 {
		t.Errorf("polls=%d, want >=1", stats.Polls)
	}
}

// --- TestMultiDRFTwoTenantsAsymmetricShare -----------------------------------

func TestMultiDRFTwoTenantsAsymmetricShare(t *testing.T) {
	c := newTestClient(t)
	target := makeMultiTarget(t, func(tc *bp.TargetConfig) {
		tc.TenantWeights = map[string]float64{"heavy": 1.0, "light": 1.0}
		tc.PriorityMinimums = map[int]float64{1: 1.0, 2: 0.0, 3: 0.0}
	})
	cleanupTarget(t, c, target)
	defer cleanupTarget(t, c, target)

	var grantsHeavy, grantsLight atomic.Int64
	deadline := time.Now().Add(1500 * time.Millisecond)

	tenantLoop := func(tenant string, tokens float64) {
		idx := 0
		for time.Now().Before(deadline) {
			idx++
			_, err := bp.QueuedAcquire(context.Background(), c, target, bp.QueuedAcquireRequest{
				TenantID:      tenant,
				Priority:      1,
				ResourceCosts: map[string]float64{"rpm": 1.0, "tpm": tokens},
				RequestID:     tenant + ":" + strconv.Itoa(idx),
				TimeoutS:      0.001,
				Blocking:      false,
			}, nil)
			if err == nil {
				if tenant == "heavy" {
					grantsHeavy.Add(1)
				} else {
					grantsLight.Add(1)
				}
				continue
			}
			if retry, ok := bp.IsDenied(err); ok {
				sleep := time.Duration(retry) * time.Millisecond
				if sleep > 100*time.Millisecond {
					sleep = 100 * time.Millisecond
				}
				if sleep < 5*time.Millisecond {
					sleep = 5 * time.Millisecond
				}
				time.Sleep(sleep)
			}
		}
	}

	specs := []struct {
		tenant string
		tokens float64
	}{
		{"heavy", 800.0},
		{"heavy", 800.0},
		{"light", 80.0},
		{"light", 80.0},
	}
	var wg sync.WaitGroup
	for _, spec := range specs {
		wg.Add(1)
		go func(tenant string, tokens float64) {
			defer wg.Done()
			tenantLoop(tenant, tokens)
		}(spec.tenant, spec.tokens)
	}
	wg.Wait()

	gh := grantsHeavy.Load()
	gl := grantsLight.Load()
	// Under DRF the heavy (TPM-dominant) tenant should net fewer grants than
	// the light (RPM-dominant) tenant, light's per-call share-cost is ~4x
	// smaller.
	if gh >= gl {
		t.Errorf("DRF should give heavy fewer grants than light; got heavy=%d light=%d", gh, gl)
	}
	// Heavy must still get some grants (no starvation).
	if gh == 0 {
		t.Errorf("heavy starved: heavy=%d light=%d", gh, gl)
	}
}
