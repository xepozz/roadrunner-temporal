package rrtemporal

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubConfigurer struct {
	sections map[string]bool
}

func (c stubConfigurer) UnmarshalKey(_ string, out any) error {
	if cfg, ok := out.(**Config); ok {
		*cfg = &Config{}
	}
	return nil
}

func (c stubConfigurer) Has(name string) bool           { return c.sections[name] }
func (c stubConfigurer) GracefulTimeout() time.Duration { return time.Minute }
func (c stubConfigurer) RRVersion() string              { return "2025.1.0" }
func (c stubConfigurer) Experimental() bool             { return false }

type stubLogger struct{}

func (stubLogger) NamedLogger(string) *slog.Logger { return slog.Default() }

func TestInit_RequiresRPCPlugin(t *testing.T) {
	cfg := stubConfigurer{sections: map[string]bool{pluginName: true}}

	err := (&Plugin{}).Init(cfg, stubLogger{}, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), rpcPluginName)
}

func TestInit_PassesWithRPCPlugin(t *testing.T) {
	cfg := stubConfigurer{sections: map[string]bool{pluginName: true, rpcPluginName: true}}

	require.NoError(t, (&Plugin{}).Init(cfg, stubLogger{}, nil))
}
