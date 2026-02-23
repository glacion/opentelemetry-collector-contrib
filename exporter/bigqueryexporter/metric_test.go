// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/testdata"
)

func TestMetricsToRowsAllTypes(t *testing.T) {
	md := testdata.GeneratMetricsAllTypesWithSampleDatapoints()
	rows := metricsToRows(md)
	require.Len(t, rows, 12)

	types := map[string]int{}
	for _, r := range rows {
		types[r["metric_type"].(string)]++
	}
	assert.Equal(t, 4, types["GAUGE"])
	assert.Equal(t, 4, types["SUM"])
	assert.Equal(t, 2, types["HISTOGRAM"])
	assert.Equal(t, 2, types["SUMMARY"])

	for _, r := range rows {
		assert.Contains(t, r["resource_attributes"].(string), "resource-attr")
		assert.Empty(t, r["resource_schema_url"])
		assert.Empty(t, r["scope_schema_url"])
		assert.IsType(t, int64(0), r["flags"])
	}
}

func TestMetricsToRowsGaugeValues(t *testing.T) {
	md := testdata.GenerateMetricsOneMetric()
	rows := metricsToRows(md)
	require.Len(t, rows, 2)

	for _, r := range rows {
		assert.Equal(t, "SUM", r["metric_type"])
		assert.NotNil(t, r["value_int"])
		assert.Equal(t, "CUMULATIVE", r["aggregation_temporality"])
		assert.True(t, r["is_monotonic"].(bool))
	}
}

func TestMetricsToRowsEmpty(t *testing.T) {
	assert.Empty(t, metricsToRows(pmetric.NewMetrics()))
}

func TestMetricsJSONDefaults(t *testing.T) {
	assert.Equal(t, "[]", bucketCountsToJSON(nil))
	assert.Equal(t, "[]", explicitBoundsToJSON(nil))
	assert.Equal(t, "[]", quantilesToJSON(pmetric.NewSummaryDataPointValueAtQuantileSlice()))
	assert.Equal(t, "[]", exemplarsToJSON(pmetric.NewExemplarSlice()))
}
