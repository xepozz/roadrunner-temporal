package internal

import (
	"errors"
	"testing"

	json "github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	bindings "go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type stubWorkflowEnvironment struct {
	bindings.WorkflowEnvironment

	info             *workflow.Info
	dataConverter    converter.DataConverter
	failureConverter converter.FailureConverter
}

func (e *stubWorkflowEnvironment) WorkflowInfo() *workflow.Info { return e.info }
func (e *stubWorkflowEnvironment) GetDataConverter() converter.DataConverter {
	return e.dataConverter
}
func (e *stubWorkflowEnvironment) GetFailureConverter() converter.FailureConverter {
	return e.failureConverter
}

func TestLocalActivityParams_FailureConverterDoesNotPanic(t *testing.T) {
	env := &stubWorkflowEnvironment{
		info:             &workflow.Info{TaskQueueName: "tq"},
		dataConverter:    converter.GetDefaultDataConverter(),
		failureConverter: temporal.GetDefaultFailureConverter(),
	}
	params := ExecuteLocalActivity{Name: "X"}.LocalActivityParams(
		env, func() {}, &commonpb.Payloads{}, &commonpb.Header{},
	)

	require.NotPanics(t, func() {
		_ = params.FailureConverter.ErrorToFailure(errors.New("boom"))
	})
}

func TestCancelWorkflow_CauseOnTheWire(t *testing.T) {
	options, err := json.Marshal(CancelWorkflow{RunID: "run-1", Cause: "because"})
	require.NoError(t, err)
	require.JSONEq(t, `{"runId":"run-1","cause":"because"}`, string(options))
}

func TestCancelWorkflow_EmptyCauseIsOmitted(t *testing.T) {
	options, err := json.Marshal(CancelWorkflow{RunID: "run-1"})
	require.NoError(t, err)
	require.JSONEq(t, `{"runId":"run-1"}`, string(options))
}

func TestCancelExternalWorkflow_ReasonOnTheWire(t *testing.T) {
	options, err := json.Marshal(CancelExternalWorkflow{
		Namespace:  "ns",
		WorkflowID: "wf-1",
		RunID:      "run-1",
		Reason:     "because",
	})
	require.NoError(t, err)
	require.JSONEq(t, `{"namespace":"ns","workflowID":"wf-1","runID":"run-1","reason":"because"}`, string(options))
}

func TestCancelExternalWorkflow_EmptyReasonIsOmitted(t *testing.T) {
	options, err := json.Marshal(CancelExternalWorkflow{Namespace: "ns", WorkflowID: "wf-1", RunID: "run-1"})
	require.NoError(t, err)
	require.JSONEq(t, `{"namespace":"ns","workflowID":"wf-1","runID":"run-1"}`, string(options))
}

func TestCancelExternalWorkflow_ReasonDecodedFromWorkerPayload(t *testing.T) {
	decoded, err := InitCommand("CancelExternalWorkflow")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal([]byte(`{"namespace":"ns","workflowID":"wf-1","runID":"run-1","reason":"because"}`), &decoded))
	require.Equal(t, "because", decoded.(*CancelExternalWorkflow).Reason)
}
