// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// versionConn implements driver.Conn and only answers ServerVersion.
type versionConn struct {
	driver.Conn
	version *driver.ServerVersion
	err     error
}

func (c versionConn) ServerVersion() (*driver.ServerVersion, error) { return c.version, c.err }

func TestTimeSeriesEngineSetting(t *testing.T) {
	version := func(major, minor uint64) *driver.ServerVersion {
		return &driver.ServerVersion{Version: proto.Version{Major: major, Minor: minor}}
	}

	assert.Equal(t, "allow_experimental_time_series_table", timeSeriesEngineSetting(versionConn{version: version(26, 6)}))
	assert.Equal(t, "allow_experimental_time_series_table", timeSeriesEngineSetting(versionConn{version: version(26, 8)}))
	assert.Equal(t, "enable_time_series_table", timeSeriesEngineSetting(versionConn{version: version(26, 9)}))
	assert.Equal(t, "enable_time_series_table", timeSeriesEngineSetting(versionConn{version: version(27, 1)}))
	assert.Equal(t, "allow_experimental_time_series_table", timeSeriesEngineSetting(versionConn{err: errors.New("no handshake")}))
}

func TestTimeSeriesModelSetColumns(t *testing.T) {
	SetLogger(zap.NewNop())

	m := NewTimeSeriesModel("otel", "otel_metrics")
	require.Error(t, m.Insert(context.Background(), versionConn{}, pmetric.NewMetrics()), "Insert before SetColumns must fail")

	m.SetColumns(SamplesColumn, MetricFamilyColumn)
	assert.Contains(t, m.insertSQL, `INSERT INTO "otel"."otel_metrics"`)
	assert.Contains(t, m.insertSQL, "samples")
	assert.Contains(t, m.metaInsertSQL, `"metric_family"`)

	m.SetColumns(LegacySamplesColumn, "")
	assert.Contains(t, m.insertSQL, "time_series")
	assert.Empty(t, m.metaInsertSQL)
}

func buildTimeSeriesTestMetrics(ts time.Time) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test-service")
	rm.Resource().Attributes().PutStr("service.instance.id", "instance-1")
	// Non-identifying resource attribute so target_info is emitted.
	rm.Resource().Attributes().PutStr("host.name", "test-host")
	sm := rm.ScopeMetrics().AppendEmpty()

	gauge := sm.Metrics().AppendEmpty()
	gauge.SetName("test.gauge")
	gaugeDP := gauge.SetEmptyGauge().DataPoints().AppendEmpty()
	gaugeDP.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	gaugeDP.SetDoubleValue(1.5)
	gaugeDP.Attributes().PutStr("attr", "gauge-val")

	sum := sm.Metrics().AppendEmpty()
	sum.SetName("test.requests")
	sumData := sum.SetEmptySum()
	sumData.SetIsMonotonic(true)
	sumData.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sumDP := sumData.DataPoints().AppendEmpty()
	sumDP.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	sumDP.SetDoubleValue(100)

	hist := sm.Metrics().AppendEmpty()
	hist.SetName("test.latency")
	histData := hist.SetEmptyHistogram()
	histData.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	histDP := histData.DataPoints().AppendEmpty()
	histDP.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	histDP.SetCount(3)
	histDP.SetSum(0.5)
	histDP.ExplicitBounds().FromRaw([]float64{0.1, 1})
	histDP.BucketCounts().FromRaw([]uint64{1, 1, 1})

	expHist := sm.Metrics().AppendEmpty()
	expHist.SetName("test.exp.latency")
	expHistData := expHist.SetEmptyExponentialHistogram()
	expHistData.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	expHistDP := expHistData.DataPoints().AppendEmpty()
	expHistDP.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	expHistDP.SetScale(2)
	expHistDP.SetCount(2)
	expHistDP.SetSum(1)
	expHistDP.Positive().SetOffset(0)
	expHistDP.Positive().BucketCounts().FromRaw([]uint64{1, 1})

	summary := sm.Metrics().AppendEmpty()
	summary.SetName("test.summary")
	summaryDP := summary.SetEmptySummary().DataPoints().AppendEmpty()
	summaryDP.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	summaryDP.SetCount(5)
	summaryDP.SetSum(10)
	quantile := summaryDP.QuantileValues().AppendEmpty()
	quantile.SetQuantile(0.99)
	quantile.SetValue(4)

	return md
}

func TestToTimeSeriesRows(t *testing.T) {
	SetLogger(zap.NewNop())

	ts := time.UnixMilli(1700000000000).UTC()
	md := buildTimeSeriesTestMetrics(ts)

	rows, _, droppedExp, _, err := convertToTimeSeries(md)
	require.NoError(t, err)
	require.Equal(t, 1, droppedExp, "exponential histogram should be dropped")

	byName := make(map[string][]timeSeriesRow)
	for _, row := range rows {
		byName[row.metricName] = append(byName[row.metricName], row)

		// No row may carry __name__ as a tag or an empty metric name.
		assert.NotContains(t, row.tags, "__name__")
		assert.NotEmpty(t, row.metricName)
		require.NotEmpty(t, row.samples)
	}

	// Gauge keeps its value and datapoint attribute as a tag.
	gaugeRows := byName["test_gauge"]
	require.Len(t, gaugeRows, 1)
	assert.Equal(t, "gauge-val", gaugeRows[0].tags["attr"])
	assert.Equal(t, "test-service", gaugeRows[0].tags["job"])
	assert.Equal(t, "instance-1", gaugeRows[0].tags["instance"])
	require.Len(t, gaugeRows[0].samples, 1)
	assert.Equal(t, []any{ts, 1.5}, gaugeRows[0].samples[0])

	// Monotonic cumulative sum becomes a counter with the _total suffix.
	require.Len(t, byName["test_requests_total"], 1)

	// Classic histogram decomposes into _bucket/_sum/_count series.
	assert.Len(t, byName["test_latency_bucket"], 3) // le=0.1, le=1, le=+Inf
	require.Len(t, byName["test_latency_sum"], 1)
	require.Len(t, byName["test_latency_count"], 1)

	// Summary decomposes into quantile series plus _sum/_count.
	summaryRows := byName["test_summary"]
	require.Len(t, summaryRows, 1)
	assert.Equal(t, "0.99", summaryRows[0].tags["quantile"])
	require.Len(t, byName["test_summary_sum"], 1)
	require.Len(t, byName["test_summary_count"], 1)

	// Resource attributes surface through the target_info series.
	require.Len(t, byName["target_info"], 1)

	// The exponential histogram must not appear as float samples.
	for name := range byName {
		assert.NotContains(t, name, "test_exp_latency")
	}
}

func TestToMetadataRows(t *testing.T) {
	SetLogger(zap.NewNop())

	md := buildTimeSeriesTestMetrics(time.UnixMilli(1700000000000).UTC())
	_, rows, _, _, err := convertToTimeSeries(md)
	require.NoError(t, err)

	typesByFamily := make(map[string]string, len(rows))
	for _, row := range rows {
		require.NotEmpty(t, row.metricFamily)
		typesByFamily[row.metricFamily] = row.metricType
	}
	// One row per metric family, deduplicated.
	require.Len(t, rows, len(typesByFamily))

	assert.Equal(t, "gauge", typesByFamily["test_gauge"])
	assert.Equal(t, "counter", typesByFamily["test_requests_total"])
	assert.Equal(t, "histogram", typesByFamily["test_latency"])
	assert.Equal(t, "summary", typesByFamily["test_summary"])
}

func TestToTimeSeriesRowsDropsDeltaTemporality(t *testing.T) {
	SetLogger(zap.NewNop())

	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	sum := sm.Metrics().AppendEmpty()
	sum.SetName("delta.sum")
	sumData := sum.SetEmptySum()
	sumData.SetIsMonotonic(true)
	sumData.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dp := sumData.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp.SetDoubleValue(1)

	rows, _, droppedExp, droppedDelta, err := convertToTimeSeries(md)
	require.NoError(t, err)
	assert.Empty(t, rows)
	assert.Zero(t, droppedExp)
	assert.Equal(t, 1, droppedDelta)
}

func TestConvertUnitScopeStaleAndInt(t *testing.T) {
	SetLogger(zap.NewNop())

	ts := time.UnixMilli(1700000000000).UTC()
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "svc")
	rm.Resource().Attributes().PutStr("host.name", "host-a")
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("scope-a")
	sm.Scope().SetVersion("v1")

	gauge := sm.Metrics().AppendEmpty()
	gauge.SetName("room.temp")
	gauge.SetUnit("Cel")
	dp := gauge.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	dp.SetIntValue(21)
	dp.Attributes().PutStr("room", "lab")

	hist := sm.Metrics().AppendEmpty()
	hist.SetName("smoke.latency")
	hist.SetUnit("s")
	histData := hist.SetEmptyHistogram()
	histData.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	histDP := histData.DataPoints().AppendEmpty()
	histDP.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	histDP.SetCount(1)
	histDP.SetSum(0.2)
	histDP.ExplicitBounds().FromRaw([]float64{1})
	histDP.BucketCounts().FromRaw([]uint64{1, 0})
	histDP.SetFlags(histDP.Flags().WithNoRecordedValue(true))

	sum := sm.Metrics().AppendEmpty()
	sum.SetName("queue.size")
	sumData := sum.SetEmptySum()
	sumData.SetIsMonotonic(false)
	sumData.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sumDP := sumData.DataPoints().AppendEmpty()
	sumDP.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	sumDP.SetDoubleValue(3)

	rows, meta, droppedExp, droppedDelta, err := convertToTimeSeries(md)
	require.NoError(t, err)
	assert.Zero(t, droppedExp)
	assert.Zero(t, droppedDelta)

	byName := map[string]timeSeriesRow{}
	for _, row := range rows {
		byName[row.metricName] = row
	}

	gaugeRow := byName["room_temp_celsius"]
	assert.Equal(t, "lab", gaugeRow.tags["room"])
	assert.Equal(t, "svc", gaugeRow.tags["job"])
	assert.Equal(t, "scope-a", gaugeRow.tags["otel_scope_name"])
	assert.Equal(t, "v1", gaugeRow.tags["otel_scope_version"])
	require.Len(t, gaugeRow.samples, 1)
	assert.Equal(t, float64(21), gaugeRow.samples[0][1])

	require.Contains(t, byName, "smoke_latency_seconds_bucket")
	assert.Equal(t, math.Float64bits(staleNaN), math.Float64bits(byName["smoke_latency_seconds_sum"].samples[0][1].(float64)))
	require.Contains(t, byName, "queue_size")
	assert.NotContains(t, byName, "queue_size_total")
	assert.Equal(t, "host-a", byName["target_info"].tags["host_name"])

	types := map[string]string{}
	units := map[string]string{}
	for _, row := range meta {
		types[row.metricFamily] = row.metricType
		units[row.metricFamily] = row.unit
	}
	assert.Equal(t, "gauge", types["room_temp_celsius"])
	assert.Equal(t, "celsius", units["room_temp_celsius"])
	assert.Equal(t, "histogram", types["smoke_latency_seconds"])
	assert.Equal(t, "seconds", units["smoke_latency_seconds"])
	assert.Equal(t, "gauge", types["queue_size"])
}
