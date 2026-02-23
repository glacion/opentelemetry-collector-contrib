// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/testdata"
)

func TestTracesToRows(t *testing.T) {
	td := testdata.GenerateTracesOneSpan()
	rows := tracesToRows(td)
	require.Len(t, rows, 1)

	row := rows[0]
	assert.Equal(t, "operationA", row["name"])
	assert.Equal(t, "ERROR", row["status_code"])
	assert.Equal(t, "status-cancelled", row["status_message"])
	assert.Contains(t, row["resource_attributes"].(string), "resource-attr")
	assert.Contains(t, row["events"].(string), "event-with-attr")
	// New fields — testdata generators set dropped counts to 1
	assert.Equal(t, int64(0), row["flags"])
	assert.IsType(t, int64(0), row["dropped_attributes_count"])
	assert.IsType(t, int64(0), row["dropped_events_count"])
	assert.IsType(t, int64(0), row["dropped_links_count"])
	assert.Empty(t, row["resource_schema_url"])
	assert.Empty(t, row["scope_schema_url"])
	assert.Contains(t, row["events"].(string), "dropped_attributes_count")
}

func TestTracesToRowsMultipleSpans(t *testing.T) {
	td := testdata.GenerateTracesTwoSpansSameResource()
	rows := tracesToRows(td)
	require.Len(t, rows, 2)

	assert.Equal(t, "operationA", rows[0]["name"])
	assert.Equal(t, "operationB", rows[1]["name"])
	assert.Contains(t, rows[1]["links"].(string), "trace_id")
}

func TestTracesToRowsMultipleResources(t *testing.T) {
	td := testdata.GenerateTracesTwoSpansSameResourceOneDifferent()
	rows := tracesToRows(td)
	require.Len(t, rows, 3)
}

func TestTracesToRowsEmpty(t *testing.T) {
	assert.Empty(t, tracesToRows(testdata.GenerateTracesNoLibraries()))
}
