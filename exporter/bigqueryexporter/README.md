# BigQuery Exporter

| Status        |                       |
|---------------|-----------------------|
| Stability     | [development]         |
| Distributions | []                    |
| Issues        | -                     |
| Code Owners   | [@glacion]            |

[development]: https://github.com/open-telemetry/opentelemetry-collector/blob/main/docs/component-stability.md#development
[@glacion]: https://github.com/glacion

Exports traces, metrics, and logs to [Google BigQuery](https://cloud.google.com/bigquery)
using the [Storage Write API](https://cloud.google.com/bigquery/docs/write-api).

The exporter requires an existing BigQuery dataset. Tables are created automatically
if they do not exist, with ingestion-time partitioning.

## Configuration

| Field                         | Type     | Default   | Required | Description                                  |
|-------------------------------|----------|-----------|----------|----------------------------------------------|
| `dataset.project`             | string   |           | No       | GCP project ID (detected from ADC if omitted)|
| `dataset.id`                  | string   |           | Yes      | BigQuery dataset ID                          |
| `dataset.trace_table`         | string   | `trace`   | No       | Table name for traces                        |
| `dataset.metric_table`        | string   | `metric`  | No       | Table name for metrics                       |
| `dataset.log_table`           | string   | `log`     | No       | Table name for logs                          |
| `timeout`                     | duration | `30s`     | No       | Timeout for BigQuery API calls               |
| `retry_on_failure.enabled`    | bool     | `true`    | No       | Enable retry on failure                      |
| `sending_queue`               | object   | disabled  | No       | Queue/batch configuration                    |

Dataset and table identifiers must match `^[A-Za-z_][A-Za-z0-9_]*$` and be at most 1024 characters.

Authentication uses [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials).
If `dataset.project` is omitted, the project ID is resolved from `GOOGLE_CLOUD_PROJECT`,
`GCLOUD_PROJECT`, or `GCP_PROJECT` environment variables, or from the ADC credentials.

## Example

```yaml
exporters:
  bigquery:
    dataset:
      project: my-gcp-project
      id: otel_dataset
    timeout: 30s
    retry_on_failure:
      enabled: true
      initial_interval: 5s
      max_interval: 30s
    sending_queue:
      num_consumers: 10
      queue_size: 1000
```

Minimal (project detected from ADC):
```yaml
exporters:
  bigquery:
    dataset:
      id: otel_dataset
```

## Schema

### Traces

| Column | Type | Description |
|--------|------|-------------|
| `trace_id` | STRING | W3C trace identifier |
| `span_id` | STRING | Unique span identifier |
| `parent_span_id` | STRING | Parent span identifier |
| `trace_state` | STRING | W3C trace state |
| `name` | STRING | Span operation name |
| `kind` | STRING | INTERNAL, SERVER, CLIENT, PRODUCER, CONSUMER, UNSPECIFIED |
| `start_time` | TIMESTAMP | Span start time |
| `end_time` | TIMESTAMP | Span end time |
| `status_code` | STRING | OK, ERROR, UNSET |
| `status_message` | STRING | Status description |
| `flags` | INTEGER | W3C trace flags |
| `dropped_attributes_count` | INTEGER | Number of dropped span attributes |
| `dropped_events_count` | INTEGER | Number of dropped events |
| `dropped_links_count` | INTEGER | Number of dropped links |
| `resource_attributes` | JSON | Resource attributes |
| `resource_schema_url` | STRING | Resource schema URL |
| `span_attributes` | JSON | Span attributes |
| `events` | JSON | Span events with timestamp, name, attributes, dropped_attributes_count |
| `links` | JSON | Span links with trace_id, span_id, trace_state, attributes, dropped_attributes_count, flags |
| `instrumentation_scope` | JSON | Instrumentation scope (name, version, attributes) |
| `scope_schema_url` | STRING | Scope schema URL |

### Metrics

| Column | Type | Description |
|--------|------|-------------|
| `metric_name` | STRING | Metric name |
| `metric_description` | STRING | Metric description |
| `metric_unit` | STRING | Metric unit |
| `metric_type` | STRING | GAUGE, SUM, HISTOGRAM, SUMMARY, EXPONENTIAL_HISTOGRAM |
| `aggregation_temporality` | STRING | CUMULATIVE, DELTA, UNSPECIFIED |
| `is_monotonic` | BOOLEAN | Whether the metric is monotonic |
| `datapoint_timestamp` | TIMESTAMP | Data point timestamp |
| `start_timestamp` | TIMESTAMP | Data point start timestamp |
| `value_int` | INTEGER | Integer value (gauge/sum) |
| `value_double` | FLOAT | Double value (gauge/sum) |
| `exemplars` | JSON | Exemplars with timestamp, trace_id, span_id, value, filtered_attributes |
| `flags` | INTEGER | Data point flags |
| `quantiles` | JSON | Summary quantile values |
| `count` | INTEGER | Histogram/summary count |
| `sum` | FLOAT | Histogram/summary sum |
| `min` | FLOAT | Histogram min value |
| `max` | FLOAT | Histogram max value |
| `bucket_counts` | JSON | Histogram bucket counts |
| `explicit_bounds` | JSON | Histogram explicit bounds |
| `zero_threshold` | FLOAT | Exponential histogram zero threshold |
| `resource_attributes` | JSON | Resource attributes |
| `resource_schema_url` | STRING | Resource schema URL |
| `datapoint_attributes` | JSON | Data point attributes |
| `instrumentation_scope` | JSON | Instrumentation scope |
| `scope_schema_url` | STRING | Scope schema URL |

### Logs

| Column | Type | Description |
|--------|------|-------------|
| `observed_timestamp` | TIMESTAMP | Time the log was observed |
| `log_timestamp` | TIMESTAMP | Time the log event occurred |
| `trace_id` | STRING | Associated trace identifier |
| `span_id` | STRING | Associated span identifier |
| `severity_number` | INTEGER | Severity number (1–24) |
| `severity_text` | STRING | Severity text (e.g., INFO, ERROR) |
| `body` | STRING | Log body |
| `flags` | INTEGER | Log record flags |
| `dropped_attributes_count` | INTEGER | Number of dropped attributes |
| `resource_attributes` | JSON | Resource attributes |
| `resource_schema_url` | STRING | Resource schema URL |
| `log_attributes` | JSON | Log attributes |
| `instrumentation_scope` | JSON | Instrumentation scope |
| `scope_schema_url` | STRING | Scope schema URL |

## Example Queries
For Grafana dashboard queries, see [Grafana Queries](#grafana-queries) below.

### Traces

Find traces by service name:

```sql
SELECT trace_id, name, start_time, end_time
FROM `project.dataset.trace`
WHERE _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
  AND JSON_VALUE(resource_attributes, '$.service.name') = 'my-service'
LIMIT 100;
```

Find error spans:

```sql
SELECT trace_id, span_id, name, status_message,
  TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) AS duration_ms
FROM `project.dataset.trace`
WHERE _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
  AND status_code = 'ERROR'
ORDER BY start_time DESC
LIMIT 100;
```

Find slow spans (>1s):

```sql
SELECT trace_id, span_id, name,
  TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) AS duration_ms,
  JSON_VALUE(resource_attributes, '$.service.name') AS service
FROM `project.dataset.trace`
WHERE _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
  AND TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) > 1000
ORDER BY duration_ms DESC
LIMIT 100;
```

Get a full trace by trace ID:

```sql
SELECT trace_id, span_id, parent_span_id, name, kind,
  start_time, end_time,
  TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) AS duration_ms,
  status_code, span_attributes
FROM `project.dataset.trace`
WHERE trace_id = '00000000000000000000000000000001'
ORDER BY start_time;
```

Find spans by attribute:

```sql
SELECT trace_id, name, start_time
FROM `project.dataset.trace`
WHERE _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
  AND JSON_VALUE(span_attributes, '$.http.method') = 'POST'
  AND JSON_VALUE(span_attributes, '$.http.route') = '/api/users'
LIMIT 100;
```

### Metrics

Query gauge or sum values:

```sql
SELECT datapoint_timestamp, value_int, value_double,
  JSON_VALUE(resource_attributes, '$.service.name') AS service
FROM `project.dataset.metric`
WHERE _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
  AND metric_name = 'system.cpu.utilization'
  AND metric_type = 'GAUGE'
ORDER BY datapoint_timestamp DESC
LIMIT 100;
```

Aggregate metrics over time:

```sql
SELECT
  TIMESTAMP_TRUNC(datapoint_timestamp, MINUTE) AS minute,
  AVG(value_double) AS avg_value,
  MAX(value_double) AS max_value,
  JSON_VALUE(resource_attributes, '$.service.name') AS service
FROM `project.dataset.metric`
WHERE _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
  AND metric_name = 'http.server.request.duration'
GROUP BY minute, service
ORDER BY minute DESC;
```

Query histogram data:

```sql
SELECT datapoint_timestamp, count, sum, min, max,
  bucket_counts, explicit_bounds,
  JSON_VALUE(datapoint_attributes, '$.http.method') AS method
FROM `project.dataset.metric`
WHERE _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
  AND metric_name = 'http.server.request.duration'
  AND metric_type = 'HISTOGRAM'
ORDER BY datapoint_timestamp DESC
LIMIT 100;
```

Find metrics by datapoint attribute:

```sql
SELECT metric_name, datapoint_timestamp, value_double
FROM `project.dataset.metric`
WHERE _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
  AND JSON_VALUE(datapoint_attributes, '$.http.status_code') = '500'
LIMIT 100;
```

### Logs

Find logs by severity:

```sql
SELECT observed_timestamp, severity_text, body,
  JSON_VALUE(resource_attributes, '$.service.name') AS service
FROM `project.dataset.log`
WHERE _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
  AND severity_text = 'ERROR'
ORDER BY observed_timestamp DESC
LIMIT 100;
```

Find logs by body content:

```sql
SELECT observed_timestamp, severity_text, body, log_attributes
FROM `project.dataset.log`
WHERE _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
  AND body LIKE '%timeout%'
ORDER BY observed_timestamp DESC
LIMIT 100;
```

Find logs correlated with a trace:

```sql
SELECT observed_timestamp, severity_text, body, span_id
FROM `project.dataset.log`
WHERE trace_id = '00000000000000000000000000000001'
ORDER BY observed_timestamp;
```

Severity count time series:

```sql
SELECT
  TIMESTAMP_TRUNC(observed_timestamp, MINUTE) AS minute,
  severity_text,
  COUNT(*) AS count
FROM `project.dataset.log`
WHERE _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
GROUP BY minute, severity_text
ORDER BY minute DESC, count DESC;
```

Find logs by attribute:

```sql
SELECT observed_timestamp, body,
  JSON_VALUE(log_attributes, '$.error.type') AS error_type
FROM `project.dataset.log`
WHERE _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
  AND JSON_VALUE(log_attributes, '$.error.type') IS NOT NULL
ORDER BY observed_timestamp DESC
LIMIT 100;
```

## Grafana Queries

These queries use [Grafana BigQuery data source](https://grafana.com/grafana/plugins/grafana-bigquery-datasource/) macros.
Use them in the raw SQL editor. The macros `$__timeFilter()`, `$__timeGroup()`, `$__timeFrom()`, and `$__timeTo()`
are expanded by Grafana to match the dashboard time range.

### Traces

Span rate over time (time series panel):

```sql
SELECT
  $__timeGroup(start_time, $__interval) AS time,
  JSON_VALUE(resource_attributes, '$.service.name') AS service,
  COUNT(*) AS span_count
FROM `project.dataset.trace`
WHERE $__timeFilter(start_time)
GROUP BY time, service
ORDER BY time
```

Error rate over time (time series panel):

```sql
SELECT
  $__timeGroup(start_time, $__interval) AS time,
  JSON_VALUE(resource_attributes, '$.service.name') AS service,
  COUNTIF(status_code = 'ERROR') AS errors,
  COUNT(*) AS total,
  SAFE_DIVIDE(COUNTIF(status_code = 'ERROR'), COUNT(*)) AS error_rate
FROM `project.dataset.trace`
WHERE $__timeFilter(start_time)
GROUP BY time, service
ORDER BY time
```

P50/P95/P99 latency over time (time series panel):

```sql
SELECT
  $__timeGroup(start_time, $__interval) AS time,
  JSON_VALUE(resource_attributes, '$.service.name') AS metric,
  APPROX_QUANTILES(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND), 100)[OFFSET(50)] AS p50_ms,
  APPROX_QUANTILES(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND), 100)[OFFSET(95)] AS p95_ms,
  APPROX_QUANTILES(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND), 100)[OFFSET(99)] AS p99_ms
FROM `project.dataset.trace`
WHERE $__timeFilter(start_time)
  AND kind = 'SERVER'
GROUP BY time, metric
ORDER BY time
```

Slowest endpoints (table panel):

```sql
SELECT
  name,
  JSON_VALUE(resource_attributes, '$.service.name') AS service,
  COUNT(*) AS count,
  AVG(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)) AS avg_ms,
  MAX(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)) AS max_ms,
  APPROX_QUANTILES(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND), 100)[OFFSET(95)] AS p95_ms
FROM `project.dataset.trace`
WHERE $__timeFilter(start_time)
  AND kind = 'SERVER'
GROUP BY name, service
ORDER BY p95_ms DESC
LIMIT 20
```

Trace search (table panel, use `$trace_id` dashboard variable):

```sql
SELECT trace_id, span_id, parent_span_id, name, kind,
  start_time, end_time,
  TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) AS duration_ms,
  status_code, status_message
FROM `project.dataset.trace`
WHERE trace_id = '${trace_id}'
ORDER BY start_time
```

### Metrics

Metric value over time (time series panel):

```sql
SELECT
  datapoint_timestamp AS time,
  JSON_VALUE(resource_attributes, '$.service.name') AS metric,
  value_double
FROM `project.dataset.metric`
WHERE $__timeFilter(datapoint_timestamp)
  AND metric_name = 'system.cpu.utilization'
  AND metric_type = 'GAUGE'
ORDER BY time
```

Aggregated metric over time (time series panel):

```sql
SELECT
  $__timeGroup(datapoint_timestamp, $__interval) AS time,
  JSON_VALUE(resource_attributes, '$.service.name') AS metric,
  AVG(value_double) AS avg_value,
  MAX(value_double) AS max_value
FROM `project.dataset.metric`
WHERE $__timeFilter(datapoint_timestamp)
  AND metric_name = 'http.server.request.duration'
GROUP BY time, metric
ORDER BY time
```

Counter rate (time series panel, for monotonic sums):

```sql
SELECT
  $__timeGroup(datapoint_timestamp, $__interval) AS time,
  JSON_VALUE(resource_attributes, '$.service.name') AS metric,
  SUM(value_int) AS total
FROM `project.dataset.metric`
WHERE $__timeFilter(datapoint_timestamp)
  AND metric_name = 'http.server.request.count'
  AND metric_type = 'SUM'
GROUP BY time, metric
ORDER BY time
```

Histogram stats over time (time series panel):

```sql
SELECT
  $__timeGroup(datapoint_timestamp, $__interval) AS time,
  JSON_VALUE(datapoint_attributes, '$.http.method') AS metric,
  AVG(SAFE_DIVIDE(sum, count)) AS avg_duration,
  MAX(max) AS max_duration,
  MIN(min) AS min_duration
FROM `project.dataset.metric`
WHERE $__timeFilter(datapoint_timestamp)
  AND metric_name = 'http.server.request.duration'
  AND metric_type = 'HISTOGRAM'
GROUP BY time, metric
ORDER BY time
```

### Logs

Log volume over time (time series panel):

```sql
SELECT
  $__timeGroup(observed_timestamp, $__interval) AS time,
  severity_text AS metric,
  COUNT(*) AS count
FROM `project.dataset.log`
WHERE $__timeFilter(observed_timestamp)
GROUP BY time, metric
ORDER BY time
```

Error logs over time by service (time series panel):

```sql
SELECT
  $__timeGroup(observed_timestamp, $__interval) AS time,
  JSON_VALUE(resource_attributes, '$.service.name') AS metric,
  COUNT(*) AS count
FROM `project.dataset.log`
WHERE $__timeFilter(observed_timestamp)
  AND severity_text IN ('ERROR', 'FATAL')
GROUP BY time, metric
ORDER BY time
```

Recent error logs (table panel):

```sql
SELECT
  observed_timestamp,
  JSON_VALUE(resource_attributes, '$.service.name') AS service,
  severity_text,
  body,
  trace_id
FROM `project.dataset.log`
WHERE $__timeFilter(observed_timestamp)
  AND severity_text = 'ERROR'
ORDER BY observed_timestamp DESC
LIMIT 100
```

Log search (table panel, use `$search` dashboard variable):

```sql
SELECT
  observed_timestamp,
  JSON_VALUE(resource_attributes, '$.service.name') AS service,
  severity_text,
  body,
  log_attributes
FROM `project.dataset.log`
WHERE $__timeFilter(observed_timestamp)
  AND body LIKE '%${search}%'
ORDER BY observed_timestamp DESC
LIMIT 200
```

Top log sources (pie/bar panel):

```sql
SELECT
  JSON_VALUE(resource_attributes, '$.service.name') AS service,
  severity_text,
  COUNT(*) AS count
FROM `project.dataset.log`
WHERE $__timeFilter(observed_timestamp)
GROUP BY service, severity_text
ORDER BY count DESC
LIMIT 20
```

## Integration Tests

Requires a GCP project with BigQuery enabled and Application Default Credentials configured.

```sh
RUN_BIGQUERY_INTEGRATION=1 go test -tags integration -run TestIntegration -v -count=1 ./...
```
Override the project with `BIGQUERY_PROJECT` or let it resolve from ADC.
