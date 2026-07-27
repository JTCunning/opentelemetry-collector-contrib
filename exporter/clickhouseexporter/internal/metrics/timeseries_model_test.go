// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheusremotewrite"
)

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

	settings := prometheusremotewrite.Settings{AddMetricSuffixes: true}
	tsMap, err := prometheusremotewrite.FromMetrics(md, settings)
	require.NoError(t, err)

	rows, droppedNativeHistograms := toTimeSeriesRows(tsMap)
	require.Equal(t, 1, droppedNativeHistograms, "exponential histogram should be dropped as a native histogram")

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
	rows := toMetadataRows(md, prometheusremotewrite.Settings{AddMetricSuffixes: true})

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

	tsMap, err := prometheusremotewrite.FromMetrics(md, prometheusremotewrite.Settings{AddMetricSuffixes: true})
	require.Error(t, err, "delta temporality is rejected by the translator")

	rows, dropped := toTimeSeriesRows(tsMap)
	assert.Empty(t, rows)
	assert.Zero(t, dropped)
}
