// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/testdata"
)

func TestLogsToRows(t *testing.T) {
	ld := testdata.GenerateLogsOneLogRecord()
	rows := logsToRows(ld)
	require.Len(t, rows, 1)

	row := rows[0]
	assert.Equal(t, "This is a log message", row["body"])
	assert.Equal(t, "Info", row["severity_text"])
	assert.NotEmpty(t, row["trace_id"])
	assert.NotEmpty(t, row["span_id"])
	assert.Contains(t, row["resource_attributes"].(string), "resource-attr")
	// New fields
	assert.IsType(t, int64(0), row["dropped_attributes_count"])
	assert.Empty(t, row["resource_schema_url"])
	assert.Empty(t, row["scope_schema_url"])
}

func TestLogsToRowsMultiple(t *testing.T) {
	ld := testdata.GenerateLogsManyLogRecordsSameResource(4)
	rows := logsToRows(ld)
	require.Len(t, rows, 4)

	assert.Equal(t, "This is a log message", rows[0]["body"])
	assert.Equal(t, "something happened", rows[1]["body"])
}

func TestLogsToRowsEmpty(t *testing.T) {
	assert.Empty(t, logsToRows(testdata.GenerateLogsNoLogRecords()))
}
