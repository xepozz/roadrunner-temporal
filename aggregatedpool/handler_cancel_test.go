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
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/workflow"
)

type cancelStubEnv struct {
	bindings.WorkflowEnvironment

	cause string

	gotNamespace  string
	gotWorkflowID string
	gotRunID      string
	gotReason     string
}

func (e *cancelStubEnv) WorkflowInfo() *workflow.Info {
	return &workflow.Info{WorkflowExecution: workflow.Execution{RunID: "run-1"}}
}

func (e *cancelStubEnv) GetCancelRequestedCause() string { return e.cause }

func (e *cancelStubEnv) RequestCancelExternalWorkflow(namespace, workflowID, runID, reason string, _ bindings.ResultHandler) {
	e.gotNamespace = namespace
	e.gotWorkflowID = workflowID
	e.gotRunID = runID
	e.gotReason = reason
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
		canceller: new(canceller.Canceller),
		log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestHandleCancel_PushesCauseFromEnvironment(t *testing.T) {
	for _, cause := range []string{"user asked nicely", ""} {
		env := &cancelStubEnv{cause: cause}
		wp := newCancelTestWorkflow(env)

		wp.handleCancel()

		messages := wp.mq.Messages()
		require.Len(t, messages, 1)
		require.Equal(t, internal.CancelWorkflow{RunID: "run-1", Cause: cause}, messages[0].Command)
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

	require.Equal(t, "ns", env.gotNamespace)
	require.Equal(t, "wf-1", env.gotWorkflowID)
	require.Equal(t, "run-2", env.gotRunID)
	require.Equal(t, "because", env.gotReason)
}
