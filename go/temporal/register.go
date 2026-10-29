package bptemporal

import (
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
)

// ActivityRegistry is the subset of worker.Worker the registration helper
// needs. The concrete worker.Worker satisfies it.
type ActivityRegistry interface {
	RegisterActivityWithOptions(a any, options activity.RegisterOptions)
}

// RegisterActivities binds the Gate's Acquire / Release / InlineRun methods
// onto w under the internal Backpressure activity names. Call this once per
// worker process after constructing the Gate.
func RegisterActivities(w ActivityRegistry, g *Gate) {
	w.RegisterActivityWithOptions(g.Acquire, activity.RegisterOptions{
		Name: ActivityAcquire,
	})
	w.RegisterActivityWithOptions(g.Release, activity.RegisterOptions{
		Name: ActivityRelease,
	})
	w.RegisterActivityWithOptions(g.InlineRun, activity.RegisterOptions{
		Name: ActivityInlineRun,
	})
}

// Static guard so changes to worker.Worker's interface surface us a compile
// error here rather than at usage sites.
var _ ActivityRegistry = (worker.Worker)(nil)
