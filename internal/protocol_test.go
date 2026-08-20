package internal

import (
	"encoding/json"
	"errors"
	"testing"

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

func roundTripCommand(t *testing.T, cmd any) any {
	t.Helper()

	name, err := CommandName(cmd)
	require.NoError(t, err)

	options, err := json.Marshal(cmd)
	require.NoError(t, err)

	decoded, err := InitCommand(name)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(options, &decoded))

	return decoded
}

func TestCancelWorkflow_CauseSurvivesRoundTrip(t *testing.T) {
	decoded := roundTripCommand(t, CancelWorkflow{RunID: "run-1", Cause: "because"})
	require.Equal(t, &CancelWorkflow{RunID: "run-1", Cause: "because"}, decoded)
}

func TestCancelWorkflow_EmptyCauseIsOmitted(t *testing.T) {
	options, err := json.Marshal(CancelWorkflow{RunID: "run-1"})
	require.NoError(t, err)
	require.NotContains(t, string(options), "cause")
}

func TestCancelExternalWorkflow_ReasonSurvivesRoundTrip(t *testing.T) {
	cmd := CancelExternalWorkflow{Namespace: "ns", WorkflowID: "wf-1", RunID: "run-1", Reason: "because"}
	require.Equal(t, &cmd, roundTripCommand(t, cmd))
}

func TestCancelExternalWorkflow_ReasonDecodedFromWorkerPayload(t *testing.T) {
	decoded, err := InitCommand("CancelExternalWorkflow")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal([]byte(`{"namespace":"ns","workflowID":"wf-1","runID":"run-1","reason":"because"}`), &decoded))
	require.Equal(t, "because", decoded.(*CancelExternalWorkflow).Reason)
}

func TestCancelExternalWorkflow_EmptyReasonIsOmitted(t *testing.T) {
	options, err := json.Marshal(CancelExternalWorkflow{Namespace: "ns", WorkflowID: "wf-1"})
	require.NoError(t, err)
	require.NotContains(t, string(options), "reason")
}
