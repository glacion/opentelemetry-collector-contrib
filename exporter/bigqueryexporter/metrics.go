// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/bigqueryexporter"

import (
	"encoding/json"
	"maps"
	"time"

	"cloud.google.com/go/bigquery"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

var metricsSchema = bigquery.Schema{
	{Name: "metric_name", Type: bigquery.StringFieldType, Required: true},
	{Name: "metric_description", Type: bigquery.StringFieldType, Required: false},
	{Name: "metric_unit", Type: bigquery.StringFieldType, Required: false},
	{Name: "metric_type", Type: bigquery.StringFieldType, Required: true},
	{Name: "aggregation_temporality", Type: bigquery.StringFieldType, Required: false},
	{Name: "is_monotonic", Type: bigquery.BooleanFieldType, Required: false},
	{Name: "datapoint_timestamp", Type: bigquery.TimestampFieldType, Required: true},
	{Name: "start_timestamp", Type: bigquery.TimestampFieldType, Required: false},
	{Name: "value_int", Type: bigquery.IntegerFieldType, Required: false},
	{Name: "value_double", Type: bigquery.FloatFieldType, Required: false},
	{Name: "exemplars", Type: bigquery.JSONFieldType, Required: false},
	{Name: "flags", Type: bigquery.IntegerFieldType, Required: false},
	{Name: "quantiles", Type: bigquery.JSONFieldType, Required: false},
	{Name: "count", Type: bigquery.IntegerFieldType, Required: false},
	{Name: "sum", Type: bigquery.FloatFieldType, Required: false},
	{Name: "min", Type: bigquery.FloatFieldType, Required: false},
	{Name: "max", Type: bigquery.FloatFieldType, Required: false},
	{Name: "bucket_counts", Type: bigquery.JSONFieldType, Required: false},
	{Name: "explicit_bounds", Type: bigquery.JSONFieldType, Required: false},
	{Name: "zero_threshold", Type: bigquery.FloatFieldType, Required: false},
	{Name: "resource_attributes", Type: bigquery.JSONFieldType, Required: false},
	{Name: "resource_schema_url", Type: bigquery.StringFieldType, Required: false},
	{Name: "datapoint_attributes", Type: bigquery.JSONFieldType, Required: false},
	{Name: "instrumentation_scope", Type: bigquery.JSONFieldType, Required: false},
	{Name: "scope_schema_url", Type: bigquery.StringFieldType, Required: false},
}

func metricsToRows(md pmetric.Metrics) []row {
	var rows []row
	for _, rm := range md.ResourceMetrics().All() {
		for _, sm := range rm.ScopeMetrics().All() {
			for _, metric := range sm.Metrics().All() {
				metricRows := metricToRows(metric, rm.Resource().Attributes(), rm.SchemaUrl(), sm.Scope(), sm.SchemaUrl())
				rows = append(rows, metricRows...)
			}
		}
	}
	return rows
}

func metricToRows(metric pmetric.Metric, resourceAttrs pcommon.Map, resourceSchemaURL string, scope pcommon.InstrumentationScope, scopeSchemaURL string) []row {
	baseRow := metricBaseRow(metric, resourceAttrs, resourceSchemaURL, scope, scopeSchemaURL)
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		return gaugeToRows(metric.Gauge(), baseRow)
	case pmetric.MetricTypeSum:
		return sumToRows(metric.Sum(), baseRow)
	case pmetric.MetricTypeHistogram:
		return histogramToRows(metric.Histogram(), baseRow)
	case pmetric.MetricTypeSummary:
		return summaryToRows(metric.Summary(), baseRow)
	case pmetric.MetricTypeExponentialHistogram:
		return exponentialHistogramToRows(metric.ExponentialHistogram(), baseRow)
	default:
		return nil
	}
}

func gaugeToRows(gauge pmetric.Gauge, base row) []row {
	return numberDataPointsToRows(gauge.DataPoints(), base, "GAUGE")
}

func sumToRows(sum pmetric.Sum, base row) []row {
	base["aggregation_temporality"] = aggregationTemporalityToString(sum.AggregationTemporality())
	base["is_monotonic"] = sum.IsMonotonic()
	return numberDataPointsToRows(sum.DataPoints(), base, "SUM")
}

func histogramToRows(hist pmetric.Histogram, base row) []row {
	dps := hist.DataPoints()
	rows := make([]row, 0, dps.Len())

	base["aggregation_temporality"] = aggregationTemporalityToString(hist.AggregationTemporality())

	for _, dp := range dps.All() {
		r := cloneMetricRow(base, "HISTOGRAM")
		setCommonDataPointFields(r, dp.Timestamp(), dp.StartTimestamp(), dp.Flags(), dp.Attributes())
		r["exemplars"] = exemplarsToJSON(dp.Exemplars())
		r["count"] = dp.Count()
		if dp.HasSum() {
			r["sum"] = dp.Sum()
		}
		if dp.HasMin() {
			r["min"] = dp.Min()
		}
		if dp.HasMax() {
			r["max"] = dp.Max()
		}
		r["bucket_counts"] = bucketCountsToJSON(dp.BucketCounts().AsRaw())
		r["explicit_bounds"] = explicitBoundsToJSON(dp.ExplicitBounds().AsRaw())
		rows = append(rows, r)
	}
	return rows
}

func summaryToRows(summary pmetric.Summary, base row) []row {
	dps := summary.DataPoints()
	rows := make([]row, 0, dps.Len())

	for _, dp := range dps.All() {
		r := cloneMetricRow(base, "SUMMARY")
		setCommonDataPointFields(r, dp.Timestamp(), dp.StartTimestamp(), dp.Flags(), dp.Attributes())
		r["count"] = dp.Count()
		r["sum"] = dp.Sum()
		r["quantiles"] = quantilesToJSON(dp.QuantileValues())
		rows = append(rows, r)
	}

	return rows
}

func exponentialHistogramToRows(hist pmetric.ExponentialHistogram, base row) []row {
	dps := hist.DataPoints()
	rows := make([]row, 0, dps.Len())
	base["aggregation_temporality"] = aggregationTemporalityToString(hist.AggregationTemporality())
	for _, dp := range dps.All() {
		r := cloneMetricRow(base, "EXPONENTIAL_HISTOGRAM")
		setCommonDataPointFields(r, dp.Timestamp(), dp.StartTimestamp(), dp.Flags(), dp.Attributes())
		r["exemplars"] = exemplarsToJSON(dp.Exemplars())
		r["count"] = dp.Count()
		if dp.HasSum() {
			r["sum"] = dp.Sum()
		}
		if dp.HasMin() {
			r["min"] = dp.Min()
		}
		if dp.HasMax() {
			r["max"] = dp.Max()
		}
		r["zero_threshold"] = dp.ZeroThreshold()
		r["bucket_counts"] = exponentialBucketInfoToJSON(dp)
		rows = append(rows, r)
	}
	return rows
}

func setCommonDataPointFields(row row, ts, start pcommon.Timestamp, flags pmetric.DataPointFlags, attrs pcommon.Map) {
	row["datapoint_timestamp"] = ts.AsTime()
	row["start_timestamp"] = start.AsTime()
	row["flags"] = int64(flags)
	row["datapoint_attributes"] = attributesToJSON(attrs)
}

func metricBaseRow(metric pmetric.Metric, resourceAttrs pcommon.Map, resourceSchemaURL string, scope pcommon.InstrumentationScope, scopeSchemaURL string) row {
	return row{
		"metric_name":             metric.Name(),
		"metric_description":      metric.Description(),
		"metric_unit":             metric.Unit(),
		"metric_type":             "",
		"aggregation_temporality": "",
		"is_monotonic":            false,
		"datapoint_timestamp":     time.Time{},
		"start_timestamp":         time.Time{},
		"value_int":               nil,
		"value_double":            nil,
		"exemplars":               "[]",
		"flags":                   int64(0),
		"quantiles":               "[]",
		"count":                   nil,
		"sum":                     nil,
		"min":                     nil,
		"max":                     nil,
		"bucket_counts":           "[]",
		"explicit_bounds":         "[]",
		"zero_threshold":          nil,
		"resource_attributes":     attributesToJSON(resourceAttrs),
		"resource_schema_url":     resourceSchemaURL,
		"datapoint_attributes":    attributesToJSON(pcommon.NewMap()),
		"instrumentation_scope":   scopeToJSON(scope),
		"scope_schema_url":        scopeSchemaURL,
	}
}

func cloneMetricRow(base row, metricType string) row {
	r := make(row, len(base))
	maps.Copy(r, base)
	r["metric_type"] = metricType
	return r
}

func numberDataPointsToRows(dps pmetric.NumberDataPointSlice, base row, metricType string) []row {
	rows := make([]row, 0, dps.Len())
	for _, dp := range dps.All() {
		r := cloneMetricRow(base, metricType)
		setCommonDataPointFields(r, dp.Timestamp(), dp.StartTimestamp(), dp.Flags(), dp.Attributes())
		r["exemplars"] = exemplarsToJSON(dp.Exemplars())
		setNumberValue(r, dp)
		rows = append(rows, r)
	}
	return rows
}

func bucketCountsToJSON(values []uint64) string {
	if len(values) == 0 {
		return "[]"
	}
	return marshalJSON(values)
}

func explicitBoundsToJSON(values []float64) string {
	if len(values) == 0 {
		return "[]"
	}
	return marshalJSON(values)
}

func quantilesToJSON(qvs pmetric.SummaryDataPointValueAtQuantileSlice) string {
	if qvs.Len() == 0 {
		return "[]"
	}

	quantiles := make([]map[string]any, 0, qvs.Len())
	for _, qv := range qvs.All() {
		quantiles = append(quantiles, map[string]any{
			"quantile": qv.Quantile(),
			"value":    qv.Value(),
		})
	}

	return marshalJSON(quantiles)
}

func exponentialBucketInfoToJSON(dp pmetric.ExponentialHistogramDataPoint) string {
	bucketInfo := map[string]any{
		"scale":      dp.Scale(),
		"zero_count": dp.ZeroCount(),
		"positive": map[string]any{
			"offset":        dp.Positive().Offset(),
			"bucket_counts": dp.Positive().BucketCounts().AsRaw(),
		},
		"negative": map[string]any{
			"offset":        dp.Negative().Offset(),
			"bucket_counts": dp.Negative().BucketCounts().AsRaw(),
		},
	}
	return marshalJSON(bucketInfo)
}

func setNumberValue(row row, dp pmetric.NumberDataPoint) {
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeInt:
		row["value_int"] = dp.IntValue()
		row["value_double"] = nil
	case pmetric.NumberDataPointValueTypeDouble:
		row["value_int"] = nil
		row["value_double"] = dp.DoubleValue()
	}
}

func aggregationTemporalityToString(at pmetric.AggregationTemporality) string {
	switch at {
	case pmetric.AggregationTemporalityCumulative:
		return "CUMULATIVE"
	case pmetric.AggregationTemporalityDelta:
		return "DELTA"
	default:
		return "UNSPECIFIED"
	}
}

func exemplarsToJSON(exemplars pmetric.ExemplarSlice) string {
	if exemplars.Len() == 0 {
		return "[]"
	}

	result := make([]map[string]any, 0, exemplars.Len())
	for _, ex := range exemplars.All() {
		m := map[string]any{
			"timestamp":           ex.Timestamp().AsTime().Format(time.RFC3339Nano),
			"trace_id":            traceIDToHex(ex.TraceID()),
			"span_id":             spanIDToHex(ex.SpanID()),
			"filtered_attributes": json.RawMessage(attributesToJSON(ex.FilteredAttributes())),
		}

		switch ex.ValueType() {
		case pmetric.ExemplarValueTypeInt:
			m["value_int"] = ex.IntValue()
		case pmetric.ExemplarValueTypeDouble:
			m["value_double"] = ex.DoubleValue()
		}

		result = append(result, m)
	}

	return marshalJSON(result)
}
