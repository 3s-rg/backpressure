// Worker is a minimal Temporal worker wired up with backpressure admission.
//
// Run a Temporal dev server (temporal server start-dev) and a Valkey
// instance (docker compose up -d valkey from the repo root), then:
//
//	go run ./examples/worker
//
// In another shell, trigger the workflow:
//
//	go run ./examples/workflow
package main

import (
	"context"
	"log"
	"time"

	"github.com/valkey-io/valkey-go"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	bp "github.com/stano45/backpressure/go"
	bptemporal "github.com/stano45/backpressure/go/temporal"
)

const (
	taskQueue  = "backpressure-example"
	targetID   = "demo-api"
	activityNm = "CallUpstream"
	workflowNm = "DemoWorkflow"
)

// gate is initialized in main and read by the workflow.
var gate *bptemporal.Gate

func main() {
	vk, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{"127.0.0.1:6379"},
	})
	if err != nil {
		log.Fatalf("valkey: %v", err)
	}
	defer vk.Close()

	gate = bptemporal.NewGate(vk, bptemporal.GateOptions{
		Targets: map[string]bp.TargetConfig{
			targetID: {
				TargetID: targetID,
				Constraints: []bp.Constraint{
					{Dimension: "rps", Limit: 5},
				},
				TenantWeights: map[string]float64{
					"team_a": 1.0,
					"team_b": 1.0,
				},
			},
		},
		ActivityTargets: map[string]string{
			activityNm: targetID,
		},
		InlineActivities: map[string]any{
			activityNm: CallUpstream,
		},
	})

	tc, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalf("temporal: %v", err)
	}
	defer tc.Close()

	w := worker.New(tc, taskQueue, worker.Options{})
	bptemporal.RegisterActivities(w, gate)
	w.RegisterActivityWithOptions(CallUpstream, activity.RegisterOptions{Name: activityNm})
	w.RegisterWorkflowWithOptions(DemoWorkflow, workflow.RegisterOptions{Name: workflowNm})

	log.Printf("worker started on task queue %q", taskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("worker exited: %v", err)
	}
}

// CallUpstream pretends to call a rate-limited API.
func CallUpstream(ctx context.Context, tenantID string) (map[string]any, error) {
	time.Sleep(100 * time.Millisecond)
	return map[string]any{
		"tenant":      tenantID,
		"called_atms": time.Now().UnixMilli(),
	}, nil
}

// DemoWorkflow runs CallUpstream under backpressure admission.
func DemoWorkflow(ctx workflow.Context, tenantID string) (map[string]any, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})

	var out map[string]any
	err := bptemporal.GatedActivity(ctx, gate, bptemporal.GateParams{
		TenantID:      tenantID,
		Priority:      1,
		RequestID:     bptemporal.MakeRequestID(ctx, 1, 1),
		ResourceCosts: map[string]float64{"rps": 1.0},
	}, activityNm, &out, tenantID)
	return out, err
}
