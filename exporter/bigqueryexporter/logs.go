// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/bigqueryexporter"

import (
	"cloud.google.com/go/bigquery"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

var logsSchema = bigquery.Schema{
	{Name: "observed_timestamp", Type: bigquery.TimestampFieldType, Required: false},
	{Name: "log_timestamp", Type: bigquery.TimestampFieldType, Required: false},
	{Name: "trace_id", Type: bigquery.StringFieldType, Required: false},
	{Name: "span_id", Type: bigquery.StringFieldType, Required: false},
	{Name: "severity_number", Type: bigquery.IntegerFieldType, Required: false},
	{Name: "severity_text", Type: bigquery.StringFieldType, Required: false},
	{Name: "body", Type: bigquery.StringFieldType, Required: false},
	{Name: "flags", Type: bigquery.IntegerFieldType, Required: false},
	{Name: "dropped_attributes_count", Type: bigquery.IntegerFieldType, Required: false},
	{Name: "resource_attributes", Type: bigquery.JSONFieldType, Required: false},
	{Name: "resource_schema_url", Type: bigquery.StringFieldType, Required: false},
	{Name: "log_attributes", Type: bigquery.JSONFieldType, Required: false},
	{Name: "instrumentation_scope", Type: bigquery.JSONFieldType, Required: false},
	{Name: "scope_schema_url", Type: bigquery.StringFieldType, Required: false},
}

func logsToRows(ld plog.Logs) []row {
	var rows []row
	for _, rl := range ld.ResourceLogs().All() {
		for _, sl := range rl.ScopeLogs().All() {
			for _, lr := range sl.LogRecords().All() {
				rows = append(rows, row{
					"observed_timestamp":       lr.ObservedTimestamp().AsTime(),
					"log_timestamp":            lr.Timestamp().AsTime(),
					"trace_id":                 traceIDToHex(lr.TraceID()),
					"span_id":                  spanIDToHex(lr.SpanID()),
					"severity_number":          int64(lr.SeverityNumber()),
					"severity_text":            lr.SeverityText(),
					"body":                     bodyToString(lr.Body()),
					"flags":                    int64(uint32(lr.Flags())),
					"dropped_attributes_count": int64(lr.DroppedAttributesCount()),
					"resource_attributes":      attributesToJSON(rl.Resource().Attributes()),
					"resource_schema_url":      rl.SchemaUrl(),
					"log_attributes":           attributesToJSON(lr.Attributes()),
					"instrumentation_scope":    scopeToJSON(sl.Scope()),
					"scope_schema_url":         sl.SchemaUrl(),
				})
			}
		}
	}

	return rows
}

func bodyToString(body pcommon.Value) string {
	switch body.Type() {
	case pcommon.ValueTypeMap, pcommon.ValueTypeSlice:
		return marshalJSON(body.AsRaw())
	case pcommon.ValueTypeEmpty:
		return ""
	default:
		return body.AsString()
	}
}
