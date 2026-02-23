// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap/confmaptest"
)

func TestLoadConfig(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	t.Run("default", func(t *testing.T) {
		sub, subErr := cm.Sub("bigquery")
		require.NoError(t, subErr)

		cfg := createDefaultConfig()
		require.NoError(t, sub.Unmarshal(cfg))

		assert.Equal(t, "test-project", cfg.Dataset.Project)
		assert.Equal(t, "test_dataset", cfg.Dataset.ID)
		assert.Equal(t, "trace", cfg.Dataset.Table.Trace)
		assert.Equal(t, "metric", cfg.Dataset.Table.Metric)
		assert.Equal(t, "log", cfg.Dataset.Table.Log)
		assert.Equal(t, 30*time.Second, cfg.TimeoutConfig.Timeout)
		assert.False(t, cfg.QueueConfig.HasValue())
	})
	t.Run("no_project", func(t *testing.T) {
		sub, subErr := cm.Sub("bigquery/no_project")
		require.NoError(t, subErr)

		cfg := createDefaultConfig()
		require.NoError(t, sub.Unmarshal(cfg))

		assert.Empty(t, cfg.Dataset.Project)
		assert.Equal(t, "adc_dataset", cfg.Dataset.ID)
		assert.Equal(t, "trace", cfg.Dataset.Table.Trace)
		assert.Equal(t, "metric", cfg.Dataset.Table.Metric)
		assert.Equal(t, "log", cfg.Dataset.Table.Log)
	})
	t.Run("custom", func(t *testing.T) {
		sub, subErr := cm.Sub("bigquery/custom")
		require.NoError(t, subErr)

		cfg := createDefaultConfig()
		require.NoError(t, sub.Unmarshal(cfg))

		assert.Equal(t, "my-project", cfg.Dataset.Project)
		assert.Equal(t, "my_dataset", cfg.Dataset.ID)
		assert.Equal(t, "custom_traces", cfg.Dataset.Table.Trace)
		assert.Equal(t, "custom_metrics", cfg.Dataset.Table.Metric)
		assert.Equal(t, "custom_logs", cfg.Dataset.Table.Log)
		assert.Equal(t, 30*time.Second, cfg.TimeoutConfig.Timeout)
		assert.True(t, cfg.BackOffConfig.Enabled)
		assert.Equal(t, 5*time.Second, cfg.BackOffConfig.InitialInterval)
		assert.Equal(t, 30*time.Second, cfg.BackOffConfig.MaxInterval)
		assert.Equal(t, 300*time.Second, cfg.BackOffConfig.MaxElapsedTime)

		require.True(t, cfg.QueueConfig.HasValue())
		qcfg := cfg.QueueConfig.Get()
		assert.Equal(t, 10, qcfg.NumConsumers)
		assert.Equal(t, int64(1000), qcfg.QueueSize)
	})
}

func TestConfigValidate(t *testing.T) {
	base := createDefaultConfig()
	base.Dataset.ID = "otel_dataset"

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{
			name:    "valid",
			mutate:  func(_ *Config) {},
			wantErr: false,
		},
		{
			name: "valid without project",
			mutate: func(c *Config) {
				c.Dataset.Project = ""
			},
			wantErr: false,
		},
		{
			name: "missing dataset",
			mutate: func(c *Config) {
				c.Dataset.ID = ""
			},
			wantErr: true,
		},
		{
			name: "project has surrounding whitespace",
			mutate: func(c *Config) {
				c.Dataset.Project = " demo-project "
			},
			wantErr: true,
		},
		{
			name: "invalid dataset identifier",
			mutate: func(c *Config) {
				c.Dataset.ID = "otel-dataset"
			},
			wantErr: true,
		},
		{
			name: "invalid traces table identifier",
			mutate: func(c *Config) {
				c.Dataset.Table.Trace = "trace-events"
			},
			wantErr: true,
		},
		{
			name: "empty logs table identifier",
			mutate: func(c *Config) {
				c.Dataset.Table.Log = ""
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := *base
			tt.mutate(&cfg)
			err := cfg.Validate()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
