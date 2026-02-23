// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/bigqueryexporter"

import (
	"encoding/json"
	"time"

	"cloud.google.com/go/bigquery"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

var tracesSchema = bigquery.Schema{
	{Name: "trace_id", Type: bigquery.StringFieldType, Required: true},
	{Name: "span_id", Type: bigquery.StringFieldType, Required: true},
	{Name: "parent_span_id", Type: bigquery.StringFieldType, Required: false},
	{Name: "trace_state", Type: bigquery.StringFieldType, Required: false},
	{Name: "name", Type: bigquery.StringFieldType, Required: true},
	{Name: "kind", Type: bigquery.StringFieldType, Required: false},
	{Name: "start_time", Type: bigquery.TimestampFieldType, Required: true},
	{Name: "end_time", Type: bigquery.TimestampFieldType, Required: true},
	{Name: "status_code", Type: bigquery.StringFieldType, Required: false},
	{Name: "status_message", Type: bigquery.StringFieldType, Required: false},
	{Name: "flags", Type: bigquery.IntegerFieldType, Required: false},
	{Name: "dropped_attributes_count", Type: bigquery.IntegerFieldType, Required: false},
	{Name: "dropped_events_count", Type: bigquery.IntegerFieldType, Required: false},
	{Name: "dropped_links_count", Type: bigquery.IntegerFieldType, Required: false},
	{Name: "resource_attributes", Type: bigquery.JSONFieldType, Required: false},
	{Name: "resource_schema_url", Type: bigquery.StringFieldType, Required: false},
	{Name: "span_attributes", Type: bigquery.JSONFieldType, Required: false},
	{Name: "events", Type: bigquery.JSONFieldType, Required: false},
	{Name: "links", Type: bigquery.JSONFieldType, Required: false},
	{Name: "instrumentation_scope", Type: bigquery.JSONFieldType, Required: false},
	{Name: "scope_schema_url", Type: bigquery.StringFieldType, Required: false},
}

func tracesToRows(td ptrace.Traces) []row {
	var rows []row
	for _, rs := range td.ResourceSpans().All() {
		for _, ss := range rs.ScopeSpans().All() {
			for _, span := range ss.Spans().All() {
				rows = append(rows, row{
					"trace_id":                 traceIDToHex(span.TraceID()),
					"span_id":                  spanIDToHex(span.SpanID()),
					"parent_span_id":           spanIDToHex(span.ParentSpanID()),
					"trace_state":              span.TraceState().AsRaw(),
					"name":                     span.Name(),
					"kind":                     spanKindToString(span.Kind()),
					"start_time":               span.StartTimestamp().AsTime(),
					"end_time":                 span.EndTimestamp().AsTime(),
					"status_code":              statusCodeToString(span.Status().Code()),
					"status_message":           span.Status().Message(),
					"flags":                    int64(span.Flags()),
					"dropped_attributes_count": int64(span.DroppedAttributesCount()),
					"dropped_events_count":     int64(span.DroppedEventsCount()),
					"dropped_links_count":      int64(span.DroppedLinksCount()),
					"resource_attributes":      attributesToJSON(rs.Resource().Attributes()),
					"resource_schema_url":      rs.SchemaUrl(),
					"span_attributes":          attributesToJSON(span.Attributes()),
					"events":                   eventsToJSON(span.Events()),
					"links":                    linksToJSON(span.Links()),
					"instrumentation_scope":    scopeToJSON(ss.Scope()),
					"scope_schema_url":         ss.SchemaUrl(),
				})
			}
		}
	}

	return rows
}

func spanKindToString(kind ptrace.SpanKind) string {
	switch kind {
	case ptrace.SpanKindInternal:
		return "INTERNAL"
	case ptrace.SpanKindServer:
		return "SERVER"
	case ptrace.SpanKindClient:
		return "CLIENT"
	case ptrace.SpanKindProducer:
		return "PRODUCER"
	case ptrace.SpanKindConsumer:
		return "CONSUMER"
	default:
		return "UNSPECIFIED"
	}
}

func statusCodeToString(code ptrace.StatusCode) string {
	switch code {
	case ptrace.StatusCodeOk:
		return "OK"
	case ptrace.StatusCodeError:
		return "ERROR"
	default:
		return "UNSET"
	}
}

func eventsToJSON(events ptrace.SpanEventSlice) string {
	if events.Len() == 0 {
		return "[]"
	}
	result := make([]map[string]any, 0, events.Len())
	for _, e := range events.All() {
		result = append(result, map[string]any{
			"timestamp":                e.Timestamp().AsTime().Format(time.RFC3339Nano),
			"name":                     e.Name(),
			"attributes":               json.RawMessage(attributesToJSON(e.Attributes())),
			"dropped_attributes_count": e.DroppedAttributesCount(),
		})
	}
	return marshalJSON(result)
}

func linksToJSON(links ptrace.SpanLinkSlice) string {
	if links.Len() == 0 {
		return "[]"
	}
	result := make([]map[string]any, 0, links.Len())
	for _, l := range links.All() {
		result = append(result, map[string]any{
			"trace_id":                 traceIDToHex(l.TraceID()),
			"span_id":                  spanIDToHex(l.SpanID()),
			"trace_state":              l.TraceState().AsRaw(),
			"attributes":               json.RawMessage(attributesToJSON(l.Attributes())),
			"dropped_attributes_count": l.DroppedAttributesCount(),
			"flags":                    int64(l.Flags()),
		})
	}
	return marshalJSON(result)
}
