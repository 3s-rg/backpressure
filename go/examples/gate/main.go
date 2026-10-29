// Standalone admission-gate demo (no Temporal).
//
// Two tenants share a 5 req/s target. team_a has twice team_b's weight, so
// under sustained contention it is granted a larger share of the throughput.
//
// Requires a Valkey on localhost:6379 (from the repo root:
// docker compose up -d valkey), then:
//
//	go run ./examples/gate
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/valkey-io/valkey-go"

	bp "github.com/stano45/backpressure/go"
)

const (
	limitRPS         = 5.0
	runSeconds       = 10
	callersPerTenant = 4
	// The bucket starts full and refills at limitRPS, so a run may exceed the
	// steady-state rate by roughly one burst window. Anything beyond that
	// headroom means the cap was not enforced.
	maxAllowedRate = limitRPS * 1.5
)

type tenantStats struct {
	grants    int
	waitMsSum int
}

func main() {
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{"127.0.0.1:6379"},
	})
	if err != nil {
		log.Fatalf("valkey: %v", err)
	}
	defer client.Close()

	// A run-scoped target id so every run starts with fresh bucket state
	// instead of inheriting whatever an earlier demo left behind.
	target := bp.TargetConfig{
		TargetID:    fmt.Sprintf("demo-api-%d", time.Now().Unix()),
		Constraints: []bp.Constraint{{Dimension: "rps", Limit: limitRPS}},
		TenantWeights: map[string]float64{
			"team_a": 2.0,
			"team_b": 1.0,
		},
	}

	fmt.Printf("target %s: %.0f req/s, weights team_a=2.0 team_b=1.0, %d concurrent callers per tenant, %d s run\n",
		target.TargetID, limitRPS, callersPerTenant, runSeconds)

	ctx, cancel := context.WithTimeout(context.Background(), runSeconds*time.Second)
	defer cancel()

	var mu sync.Mutex
	stats := map[string]*tenantStats{
		"team_a": {},
		"team_b": {},
	}
	started := time.Now()

	var wg sync.WaitGroup
	for tenant := range stats {
		for c := range callersPerTenant {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for n := 0; ctx.Err() == nil; n++ {
					acquired, err := bp.QueuedAcquire(ctx, client, target, bp.QueuedAcquireRequest{
						TenantID:      tenant,
						Priority:      1,
						ResourceCosts: map[string]float64{"rps": 1.0},
						RequestID:     fmt.Sprintf("%s-%d-%d", tenant, c, n),
						TimeoutS:      30,
						Blocking:      true,
					}, nil)
					if err != nil {
						return // context deadline: stop this caller
					}
					// Permit granted and already debited: call the rate-limited
					// upstream here. On upstream failure, refund with QueuedRelease.
					mu.Lock()
					t := stats[tenant]
					t.grants++
					t.waitMsSum += acquired.WaitMs
					grants := t.grants
					mu.Unlock()
					fmt.Printf("[%5.1fs] %s granted #%d (waited %d ms, %d polls)\n",
						time.Since(started).Seconds(), tenant, grants, acquired.WaitMs, acquired.Polls)
				}
			}()
		}
	}
	wg.Wait()

	total := 0
	fmt.Println()
	fmt.Println("summary")
	fmt.Println("-------")
	for _, tenant := range []string{"team_a", "team_b"} {
		t := stats[tenant]
		total += t.grants
		meanWait := 0.0
		if t.grants > 0 {
			meanWait = float64(t.waitMsSum) / float64(t.grants)
		}
		fmt.Printf("  %s: %d grants, mean wait %.0f ms\n", tenant, t.grants, meanWait)
	}
	rate := float64(total) / runSeconds
	fmt.Printf("  total : %d grants in %d s (~%.1f/s against a %.0f/s cap)\n", total, runSeconds, rate, limitRPS)

	capOK := rate <= maxAllowedRate
	allServed := stats["team_a"].grants > 0 && stats["team_b"].grants > 0
	if capOK && allServed {
		fmt.Printf("SUCCESS: cap enforced (%.1f/s <= %.1f/s) and every tenant was served\n", rate, maxAllowedRate)
		return
	}
	fmt.Printf("FAILURE: capOK=%v allServed=%v\n", capOK, allServed)
	os.Exit(1)
}
