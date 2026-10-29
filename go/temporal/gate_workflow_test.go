package bptemporal_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	bp "github.com/stano45/backpressure/go"
	bptemporal "github.com/stano45/backpressure/go/temporal"
)

var mockAny = mock.Anything

func activityRegOpts(name string) activity.RegisterOptions {
	return activity.RegisterOptions{Name: name}
}

// TestGatedActivity_RetryThenGrant verifies the workflow-side gate retry loop:
// the first acquire returns backpressure:denied, the second succeeds, and the
// user activity is then dispatched and its result is returned.
//
// We don't need Valkey here because the testsuite lets us mock the activity
// outputs directly.
func TestGatedActivity_RetryThenGrant(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	target := bp.TargetConfig{
		TargetID:    "demo",
		Constraints: []bp.Constraint{{Dimension: "rps", Limit: 1}},
	}
	gate := bptemporal.NewGate(nil, bptemporal.GateOptions{
		Targets:         map[string]bp.TargetConfig{"demo": target},
		ActivityTargets: map[string]string{"CallUpstream": "demo"},
	})

	// The user activity. We mock its body via OnActivity below.
	CallUpstream := func(ctx context.Context, who string) (map[string]any, error) {
		return map[string]any{"called": who}, nil
	}

	wf := func(ctx workflow.Context, who string) (map[string]any, error) {
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
		})
		var out map[string]any
		err := bptemporal.GatedActivity(ctx, gate, bptemporal.GateParams{
			TenantID:       "team_a",
			Priority:       1,
			RequestID:      "req-1",
			ResourceCosts:  map[string]float64{"rps": 1.0},
			GateRetrySleep: 5 * time.Millisecond,
		}, "CallUpstream", &out, who)
		return out, err
	}

	bptemporal.RegisterActivities(env, gate)
	env.RegisterActivityWithOptions(CallUpstream, activityRegOpts("CallUpstream"))
	env.RegisterWorkflow(wf)

	// First __bp_acquire call: denied with 5ms retry hint.
	denied := temporal.NewNonRetryableApplicationError(
		"denied", bptemporal.DeniedErrorType, errors.New("denied"), int64(5),
	)
	env.OnActivity(bptemporal.ActivityAcquire, mockAny, mockAny).Return(
		bptemporal.StatsDict{}, denied,
	).Once()
	// Second __bp_acquire call: granted.
	env.OnActivity(bptemporal.ActivityAcquire, mockAny, mockAny).Return(
		bptemporal.StatsDict{Polls: 2, WaitMs: 5},
		nil,
	).Once()

	env.ExecuteWorkflow(wf, "alice")

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow not completed")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var got map[string]any
	if err := env.GetWorkflowResult(&got); err != nil {
		t.Fatalf("result: %v", err)
	}
	if got["called"] != "alice" {
		t.Errorf("called=%v", got["called"])
	}
	// Stat injection: the workflow returns a map result, so queue stats
	// should be merged in by GatedActivity.
	if got["gate_mode"] != "local-activity" {
		t.Errorf("gate_mode=%v", got["gate_mode"])
	}
}

// TestGatedActivity_MaxRetriesExceeded verifies the workflow surfaces the
// underlying error after the gate has been denied too many times.
func TestGatedActivity_MaxRetriesExceeded(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	target := bp.TargetConfig{TargetID: "demo", Constraints: []bp.Constraint{{Dimension: "rps", Limit: 1}}}
	gate := bptemporal.NewGate(nil, bptemporal.GateOptions{
		Targets:         map[string]bp.TargetConfig{"demo": target},
		ActivityTargets: map[string]string{"CallUpstream": "demo"},
	})

	CallUpstream := func(ctx context.Context) error { return nil }

	wf := func(ctx workflow.Context) error {
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
		})
		return bptemporal.GatedActivity(ctx, gate, bptemporal.GateParams{
			TenantID:       "team_a",
			Priority:       1,
			RequestID:      "req-1",
			ResourceCosts:  map[string]float64{"rps": 1.0},
			MaxGateRetries: 1,
			GateRetrySleep: time.Millisecond,
		}, "CallUpstream", nil)
	}

	bptemporal.RegisterActivities(env, gate)
	env.RegisterActivity(CallUpstream)
	env.RegisterWorkflow(wf)

	denied := temporal.NewNonRetryableApplicationError(
		"denied", bptemporal.DeniedErrorType, errors.New("denied"), int64(1),
	)
	// Allow more denials than MaxGateRetries to ensure the helper actually
	// stops on its own rather than just running out of mock invocations.
	env.OnActivity(bptemporal.ActivityAcquire, mockAny, mockAny).Return(
		bptemporal.StatsDict{}, denied,
	).Times(5)

	env.ExecuteWorkflow(wf)
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow not completed")
	}
	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("expected workflow error after max retries")
	}
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || appErr.Type() != bptemporal.DeniedErrorType {
		t.Fatalf("expected DeniedErrorType, got %v", err)
	}
}
