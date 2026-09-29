package tests

import (
	"context"
	"sync"
	"testing"
	"tests/helpers"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

func Test_WorkerStopTimeoutDrainsRunningActivity(t *testing.T) {
	const activitySeconds = 8

	stopCh := make(chan struct{}, 1)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	s := helpers.NewTestServer(t, stopCh, wg, "../configs/.rr-worker-stop-timeout.yaml")

	w, err := s.Client.ExecuteWorkflow(
		context.Background(),
		client.StartWorkflowOptions{TaskQueue: "default"},
		"SlowActivityWorkflow",
		activitySeconds,
	)
	require.NoError(t, err)

	waitForEvent(t, s.Client, w, enums.EVENT_TYPE_ACTIVITY_TASK_STARTED)

	startedAt := time.Now()
	stopCh <- struct{}{}
	wg.Wait()
	elapsed := time.Since(startedAt)

	require.GreaterOrEqual(
		t,
		elapsed,
		5*time.Second,
		"shutdown returned in %s: the worker did not wait for the running activity", elapsed,
	)

	waitForEvent(t, s.Client, w, enums.EVENT_TYPE_ACTIVITY_TASK_COMPLETED)
}

func waitForEvent(t *testing.T, c client.Client, w client.WorkflowRun, eventType enums.EventType) {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if hasEvent(t, c, w, eventType) {
			return
		}

		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("no %s event in the history of %s", eventType, w.GetID())
}

func hasEvent(t *testing.T, c client.Client, w client.WorkflowRun, eventType enums.EventType) bool {
	t.Helper()

	iter := c.GetWorkflowHistory(
		context.Background(),
		w.GetID(),
		w.GetRunID(),
		false,
		enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
	)

	for iter.HasNext() {
		event, err := iter.Next()
		require.NoError(t, err)

		if event.GetEventType() == eventType {
			return true
		}
	}

	return false
}
