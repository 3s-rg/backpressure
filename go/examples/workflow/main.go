// Workflow starts a single DemoWorkflow execution against the example worker.
//
// Run a Temporal dev server (temporal server start-dev), Valkey
// (docker compose up -d valkey), and the worker (go run ./examples/worker),
// then:
//
//	go run ./examples/workflow
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.temporal.io/sdk/client"
)

const (
	taskQueue  = "backpressure-example"
	workflowNm = "DemoWorkflow"
)

func main() {
	tc, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalf("temporal: %v", err)
	}
	defer tc.Close()

	wfID := fmt.Sprintf("bp-demo-%d", time.Now().UnixNano())
	run, err := tc.ExecuteWorkflow(
		context.Background(),
		client.StartWorkflowOptions{
			ID:        wfID,
			TaskQueue: taskQueue,
		},
		workflowNm,
		"team_a",
	)
	if err != nil {
		log.Fatalf("ExecuteWorkflow: %v", err)
	}
	log.Printf("started workflow %q (run id %s)", wfID, run.GetRunID())

	var out map[string]any
	if err := run.Get(context.Background(), &out); err != nil {
		log.Fatalf("workflow result: %v", err)
	}
	log.Printf("workflow result: %+v", out)
}
