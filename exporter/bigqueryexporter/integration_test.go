// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"strings"
	"testing"

	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/testdata"
)

func TestIntegration_ExporterLifecycleAndWrites(t *testing.T) {
	fx := newIntegrationFixture(t)
	defer fx.cleanup(t)

	t.Run("dataset missing fails start", func(t *testing.T) {
		cfg := createDefaultConfig()
		cfg.Dataset.Project = fx.projectID
		cfg.Dataset.ID = temporaryDatasetID()

		exp := newBigQueryExporter(t.Context(), cfg, zap.NewNop())

		err := exp.start(t.Context(), nil)
		if err == nil {
			t.Fatal("start expected error, got nil")
		}
		if !strings.Contains(err.Error(), "dataset") {
			t.Fatalf("start error = %q, want dataset error", err.Error())
		}
	})

	t.Run("creates default tables and writes all signals", func(t *testing.T) {
		cfg := createDefaultConfig()
		cfg.Dataset.Project = fx.projectID
		cfg.Dataset.ID = fx.datasetID

		exp := newBigQueryExporter(t.Context(), cfg, zap.NewNop())
		if err := exp.start(t.Context(), nil); err != nil {
			t.Fatalf("start exporter: %v", err)
		}
		defer func() {
			if err := exp.shutdown(t.Context()); err != nil {
				t.Fatalf("shutdown exporter: %v", err)
			}
		}()

		for _, table := range []string{cfg.Dataset.Table.Trace, cfg.Dataset.Table.Metric, cfg.Dataset.Table.Log} {
			exists, err := fx.tableExists(table)
			if err != nil {
				t.Fatalf("tableExists(%s): %v", table, err)
			}
			if !exists {
				t.Fatalf("expected table %q to exist", table)
			}
		}

		if err := exp.pushTraces(t.Context(), testdata.GenerateTracesManySpansSameResource(5)); err != nil {
			t.Fatalf("push traces: %v", err)
		}
		if err := exp.pushMetrics(t.Context(), testdata.GeneratMetricsAllTypesWithSampleDatapoints()); err != nil {
			t.Fatalf("push metrics: %v", err)
		}
		if err := exp.pushLogs(t.Context(), testdata.GenerateLogsManyLogRecordsSameResource(5)); err != nil {
			t.Fatalf("push logs: %v", err)
		}

		fx.waitForRows(t, cfg.Dataset.Table.Trace, 5)
		fx.waitForRows(t, cfg.Dataset.Table.Metric, 12)
		fx.waitForRows(t, cfg.Dataset.Table.Log, 5)
	})

	t.Run("respects custom table names and accumulates multiple writes", func(t *testing.T) {
		cfg := createDefaultConfig()
		cfg.Dataset.Project = fx.projectID
		cfg.Dataset.ID = fx.datasetID
		cfg.Dataset.Table.Trace = "trace_custom"
		cfg.Dataset.Table.Metric = "metric_custom"
		cfg.Dataset.Table.Log = "log_custom"

		exp := newBigQueryExporter(t.Context(), cfg, zap.NewNop())
		if err := exp.start(t.Context(), nil); err != nil {
			t.Fatalf("start exporter: %v", err)
		}
		defer func() {
			if err := exp.shutdown(t.Context()); err != nil {
				t.Fatalf("shutdown exporter: %v", err)
			}
		}()

		for _, table := range []string{cfg.Dataset.Table.Trace, cfg.Dataset.Table.Metric, cfg.Dataset.Table.Log} {
			exists, err := fx.tableExists(table)
			if err != nil {
				t.Fatalf("tableExists(%s): %v", table, err)
			}
			if !exists {
				t.Fatalf("expected custom table %q to exist", table)
			}
		}

		for i := range 2 {
			if err := exp.pushTraces(t.Context(), testdata.GenerateTracesManySpansSameResource(3)); err != nil {
				t.Fatalf("push traces batch %d: %v", i, err)
			}
			if err := exp.pushMetrics(t.Context(), testdata.GenerateMetricsManyMetricsSameResource(3)); err != nil {
				t.Fatalf("push metrics batch %d: %v", i, err)
			}
			if err := exp.pushLogs(t.Context(), testdata.GenerateLogsManyLogRecordsSameResource(3)); err != nil {
				t.Fatalf("push logs batch %d: %v", i, err)
			}
		}

		fx.waitForRows(t, cfg.Dataset.Table.Trace, 6)
		fx.waitForRows(t, cfg.Dataset.Table.Metric, 12)
		fx.waitForRows(t, cfg.Dataset.Table.Log, 6)
	})
}
