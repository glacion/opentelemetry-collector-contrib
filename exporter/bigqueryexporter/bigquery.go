// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/bigqueryexporter"

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"golang.org/x/oauth2/google"
)

type bigQueryExporter struct {
	cfg             *Config
	logger          *zap.Logger
	project         string
	client          *bigquery.Client
	writeClient     *managedwriter.Client
	tracesAppender  *storageAppender
	metricsAppender *storageAppender
	logsAppender    *storageAppender
}

type row = map[string]bigquery.Value

type signalTarget struct {
	name     string
	tableID  string
	schema   bigquery.Schema
	appender **storageAppender
}

func newBigQueryExporter(_ context.Context, cfg *Config, logger *zap.Logger) *bigQueryExporter {
	return &bigQueryExporter{cfg: cfg, logger: logger}
}

// resolveProject returns the configured project ID, or detects it from
// environment variables / Application Default Credentials when not set.
func (e *bigQueryExporter) resolveProject(ctx context.Context) (string, error) {
	if e.cfg.Dataset.Project != "" {
		return e.cfg.Dataset.Project, nil
	}
	for _, key := range []string{"GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT", "GCP_PROJECT"} {
		if v := os.Getenv(key); v != "" {
			return v, nil
		}
	}
	creds, err := google.FindDefaultCredentials(ctx, bigquery.Scope)
	if err != nil {
		return "", fmt.Errorf("dataset.project not set and unable to detect from ADC: %w", err)
	}
	if creds.ProjectID == "" {
		return "", errors.New("dataset.project not set and ADC credentials do not contain a project ID")
	}
	return creds.ProjectID, nil
}

func (e *bigQueryExporter) start(ctx context.Context, _ component.Host) error {
	project, err := e.resolveProject(ctx)
	if err != nil {
		return err
	}
	e.project = project

	e.client, err = bigquery.NewClient(ctx, e.project)
	if err != nil {
		return fmt.Errorf("create BigQuery client: %w", err)
	}
	e.writeClient, err = newStorageWriteClient(ctx, e.project)
	if err != nil {
		return fmt.Errorf("create BigQuery Storage Write client: %w", err)
	}
	dataset := e.client.Dataset(e.cfg.Dataset.ID)
	if _, metadataErr := dataset.Metadata(ctx); metadataErr != nil {
		return fmt.Errorf("dataset %s does not exist (dataset auto-creation is disabled): %w", e.cfg.Dataset.ID, metadataErr)
	}
	for _, target := range e.signalTargets() {
		*target.appender, err = e.initTableAndAppender(ctx, target.tableID, target.schema, target.name)
		if err != nil {
			return err
		}
	}

	e.logger.Info("BigQuery exporter started", zap.String("project", e.project), zap.String("dataset", e.cfg.Dataset.ID))
	return nil
}

func (e *bigQueryExporter) signalTargets() []signalTarget {
	return []signalTarget{
		{name: "traces", tableID: e.cfg.Dataset.Table.Trace, schema: tracesSchema, appender: &e.tracesAppender},
		{name: "metrics", tableID: e.cfg.Dataset.Table.Metric, schema: metricsSchema, appender: &e.metricsAppender},
		{name: "logs", tableID: e.cfg.Dataset.Table.Log, schema: logsSchema, appender: &e.logsAppender},
	}
}

func (e *bigQueryExporter) initTableAndAppender(
	ctx context.Context,
	tableID string,
	schema bigquery.Schema,
	signal string,
) (*storageAppender, error) {
	table := e.client.Dataset(e.cfg.Dataset.ID).Table(tableID)
	if _, err := table.Metadata(ctx); err != nil {
		if err := table.Create(ctx, &bigquery.TableMetadata{
			Schema:           schema,
			TimePartitioning: &bigquery.TimePartitioning{Type: bigquery.DayPartitioningType},
		}); err != nil {
			return nil, fmt.Errorf("create %s table %s: %w", signal, tableID, err)
		}
		e.logger.Info("Created table", zap.String("signal", signal), zap.String("table", tableID))
	}

	appender, err := newStorageAppender(ctx, e.writeClient, e.project, e.cfg.Dataset.ID, tableID, schema)
	if err != nil {
		return nil, fmt.Errorf("create %s storage appender for table %s: %w", signal, tableID, err)
	}
	return appender, nil
}

func (e *bigQueryExporter) shutdown(_ context.Context) error {
	for _, target := range e.signalTargets() {
		if err := closeAppender(target.name, *target.appender); err != nil {
			return err
		}
	}

	if e.writeClient != nil {
		if err := e.writeClient.Close(); err != nil {
			return fmt.Errorf("close BigQuery Storage Write client: %w", err)
		}
	}
	if e.client != nil {
		if err := e.client.Close(); err != nil {
			return fmt.Errorf("close BigQuery client: %w", err)
		}
	}

	e.logger.Info("BigQuery exporter shut down")
	return nil
}

func closeAppender(signal string, appender *storageAppender) error {
	if appender == nil {
		return nil
	}
	if err := appender.stream.Close(); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("close %s appender: %w", signal, err)
	}
	return nil
}

func (e *bigQueryExporter) pushTraces(ctx context.Context, td ptrace.Traces) error {
	rows := tracesToRows(td)
	if len(rows) == 0 {
		return nil
	}
	if err := appendStorageRows(ctx, e.tracesAppender, rows); err != nil {
		return fmt.Errorf("append traces rows: %w", err)
	}
	return nil
}

func (e *bigQueryExporter) pushMetrics(ctx context.Context, md pmetric.Metrics) error {
	rows := metricsToRows(md)
	if len(rows) == 0 {
		return nil
	}
	if err := appendStorageRows(ctx, e.metricsAppender, rows); err != nil {
		return fmt.Errorf("append metrics rows: %w", err)
	}
	return nil
}

func (e *bigQueryExporter) pushLogs(ctx context.Context, ld plog.Logs) error {
	rows := logsToRows(ld)
	if len(rows) == 0 {
		return nil
	}
	if err := appendStorageRows(ctx, e.logsAppender, rows); err != nil {
		return fmt.Errorf("append logs rows: %w", err)
	}
	return nil
}

func marshalJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func traceIDToHex(id pcommon.TraceID) string {
	return hex.EncodeToString(id[:])
}

func spanIDToHex(id pcommon.SpanID) string {
	if id.IsEmpty() {
		return ""
	}
	return hex.EncodeToString(id[:])
}

func attributesToJSON(attrs pcommon.Map) string {
	if attrs.Len() == 0 {
		return "{}"
	}
	return marshalJSON(attrs.AsRaw())
}

func scopeToJSON(scope pcommon.InstrumentationScope) string {
	m := map[string]any{
		"name":    scope.Name(),
		"version": scope.Version(),
	}
	if scope.Attributes().Len() > 0 {
		m["attributes"] = scope.Attributes().AsRaw()
	}
	return marshalJSON(m)
}
