package aggregatedpool

import (
	"io"
	"log/slog"
	"testing"

	"github.com/roadrunner-server/pool/v2/worker"
	"github.com/stretchr/testify/require"
	"github.com/temporalio/roadrunner-temporal/v6/api"
	"github.com/temporalio/roadrunner-temporal/v6/canceller"
	"github.com/temporalio/roadrunner-temporal/v6/internal"
	"github.com/temporalio/roadrunner-temporal/v6/queue"
	"github.com/temporalio/roadrunner-temporal/v6/registry"
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/workflow"
)

type cancelStubEnv struct {
	bindings.WorkflowEnvironment

	cause string

	childNamespace  string
	childWorkflowID string
	childReason     string

	externalNamespace  string
	externalWorkflowID string
	externalRunID      string
	externalReason     string
}

func (e *cancelStubEnv) WorkflowInfo() *workflow.Info {
	return &workflow.Info{WorkflowExecution: workflow.Execution{RunID: "run-1"}}
}

func (e *cancelStubEnv) GetCancellationReason() string { return e.cause }

func (e *cancelStubEnv) ExecuteChildWorkflow(_ bindings.ExecuteWorkflowParams, _ bindings.ResultHandler, startedHandler func(bindings.WorkflowExecution, error)) {
	startedHandler(bindings.WorkflowExecution{}, nil)
}

func (e *cancelStubEnv) RequestCancelChildWorkflow(namespace, workflowID, reason string) {
	e.childNamespace = namespace
	e.childWorkflowID = workflowID
	e.childReason = reason
}

func (e *cancelStubEnv) RequestCancelExternalWorkflow(namespace, workflowID, runID, reason string, _ bindings.ResultHandler) {
	e.externalNamespace = namespace
	e.externalWorkflowID = workflowID
	e.externalRunID = runID
	e.externalReason = reason
}

type emptyPool struct {
	api.Pool
}

func (emptyPool) Workers() []*worker.Process { return nil }

func newCancelTestWorkflow(env bindings.WorkflowEnvironment) *Workflow {
	return &Workflow{
		env:       env,
		pool:      emptyPool{},
		mq:        queue.NewMessageQueue(seq),
		ids:       new(registry.IDRegistry),
		canceller: new(canceller.Canceller),
		log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestHandleCancel_PushesCauseFromEnvironment(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cause string
	}{
		{name: "with cause", cause: "user asked nicely"},
		{name: "without cause", cause: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := &cancelStubEnv{cause: tc.cause}
			wp := newCancelTestWorkflow(env)

			wp.handleCancel()

			messages := wp.mq.Messages()
			require.Len(t, messages, 1)
			require.Equal(t, internal.CancelWorkflow{RunID: "run-1", Cause: tc.cause}, messages[0].Command)
		})
	}
}

func TestHandleMessage_ForwardsCancelExternalWorkflowReason(t *testing.T) {
	env := &cancelStubEnv{}
	wp := newCancelTestWorkflow(env)

	require.NoError(t, wp.handleMessage(&internal.Message{
		ID: 1,
		Command: &internal.CancelExternalWorkflow{
			Namespace:  "ns",
			WorkflowID: "wf-1",
			RunID:      "run-2",
			Reason:     "because",
		},
	}))

	require.Equal(t, "ns", env.externalNamespace)
	require.Equal(t, "wf-1", env.externalWorkflowID)
	require.Equal(t, "run-2", env.externalRunID)
	require.Equal(t, "because", env.externalReason)
}

func TestHandleMessage_ChildWorkflowCancelCarriesNoInventedReason(t *testing.T) {
	env := &cancelStubEnv{cause: "because"}
	wp := newCancelTestWorkflow(env)

	require.NoError(t, wp.handleMessage(&internal.Message{
		ID: 1,
		Command: &internal.ExecuteChildWorkflow{
			Name:    "child",
			Options: bindings.WorkflowOptions{WorkflowID: "child-1", Namespace: "ns"},
		},
	}))
	require.NoError(t, wp.canceller.Cancel(1))

	require.Equal(t, "ns", env.childNamespace)
	require.Equal(t, "child-1", env.childWorkflowID)
	require.Empty(t, env.childReason, "the plugin cannot know why the worker canceled this child, so it must not reuse the workflow's own reason")
}
