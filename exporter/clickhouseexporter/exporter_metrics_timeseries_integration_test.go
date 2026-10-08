// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package clickhouseexporter

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap/zaptest"
)

func TestTimeSeriesSchema(t *testing.T) {
	container, chEnv, err := createClickhouseContainer("clickhouse/clickhouse-server:26.9-alpine")
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := container.Terminate(ctx); err != nil {
			t.Logf("terminate container: %v", err)
		}
	})

	endpoint := strings.Replace(chEnv.NativeEndpoint, "database=otel_int_test", "database=default", 1)
	const database = "otel_ts_schema"

	cfg := withDefaultConfig(func(cfg *Config) {
		cfg.Endpoint = endpoint
		cfg.Database = database
		cfg.MetricsSchema = schemaTimeSeries
		cfg.MetricsTimeSeriesTableName = "otel_metrics"
	})
	require.NoError(t, cfg.Validate())

	exp := newMetricsExporter(zaptest.NewLogger(t), cfg)
	require.NoError(t, exp.start(t.Context(), nil))
	t.Cleanup(func() {
		require.NoError(t, exp.shutdown(t.Context()))
	})

	ts := time.Now().UTC().Truncate(time.Millisecond)
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "smoke-service")
	rm.Resource().Attributes().PutStr("host.name", "smoke-host")
	sm := rm.ScopeMetrics().AppendEmpty()

	gauge := sm.Metrics().AppendEmpty()
	gauge.SetName("smoke.temperature")
	gauge.SetDescription("smoke test gauge")
	gaugeDP := gauge.SetEmptyGauge().DataPoints().AppendEmpty()
	gaugeDP.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	gaugeDP.SetDoubleValue(21.5)
	gaugeDP.Attributes().PutStr("room", "kitchen")

	sum := sm.Metrics().AppendEmpty()
	sum.SetName("smoke.requests")
	sumData := sum.SetEmptySum()
	sumData.SetIsMonotonic(true)
	sumData.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sumDP := sumData.DataPoints().AppendEmpty()
	sumDP.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	sumDP.SetDoubleValue(1234)

	hist := sm.Metrics().AppendEmpty()
	hist.SetName("smoke.latency")
	hist.SetUnit("s")
	histData := hist.SetEmptyHistogram()
	histData.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	histDP := histData.DataPoints().AppendEmpty()
	histDP.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	histDP.SetCount(3)
	histDP.SetSum(0.75)
	histDP.ExplicitBounds().FromRaw([]float64{0.1, 1})
	histDP.BucketCounts().FromRaw([]uint64{1, 1, 1})

	require.NoError(t, exp.pushMetricsData(t.Context(), md))

	var tagRows []struct {
		MetricName string            `ch:"metric_name"`
		Tags       map[string]string `ch:"tags"`
	}
	require.NoError(t, exp.db.Select(t.Context(),
		&tagRows,
		"SELECT metric_name, tags FROM timeSeriesTags("+database+".otel_metrics) ORDER BY metric_name",
	))

	names := make(map[string]map[string]string, len(tagRows))
	for _, row := range tagRows {
		names[row.MetricName] = row.Tags
	}
	t.Logf("series in tags table: %v", names)

	assert.Contains(t, names, "smoke_temperature")
	assert.Contains(t, names, "smoke_requests_total")
	assert.Contains(t, names, "smoke_latency_seconds_bucket")
	assert.Contains(t, names, "smoke_latency_seconds_sum")
	assert.Contains(t, names, "smoke_latency_seconds_count")
	assert.Contains(t, names, "target_info")
	assert.Equal(t, "kitchen", names["smoke_temperature"]["room"])
	assert.Equal(t, "smoke-service", names["smoke_temperature"]["job"])

	var sampleCount uint64
	row := exp.db.QueryRow(t.Context(), "SELECT count() FROM timeSeriesData("+database+".otel_metrics)")
	require.NoError(t, row.Scan(&sampleCount))
	// gauge(1) + sum(1) + histogram bucket(3)+sum(1)+count(1) + target_info(1) = 8
	assert.Equal(t, uint64(8), sampleCount, "expected 8 samples")

	var metaRows []struct {
		Family string `ch:"metric_family_name"`
		Type   string `ch:"type"`
		Unit   string `ch:"unit"`
		Help   string `ch:"help"`
	}
	require.NoError(t, exp.db.Select(t.Context(),
		&metaRows,
		"SELECT metric_family_name, type, unit, help FROM timeSeriesMetrics("+database+".otel_metrics) ORDER BY metric_family_name",
	))
	metaByFamily := make(map[string]string, len(metaRows))
	helpByFamily := make(map[string]string, len(metaRows))
	for _, m := range metaRows {
		metaByFamily[m.Family] = m.Type
		helpByFamily[m.Family] = m.Help
	}
	t.Logf("metadata: %v", metaByFamily)
	assert.Equal(t, "gauge", metaByFamily["smoke_temperature"])
	assert.Equal(t, "counter", metaByFamily["smoke_requests_total"])
	assert.Equal(t, "histogram", metaByFamily["smoke_latency_seconds"])
	assert.Equal(t, "smoke test gauge", helpByFamily["smoke_temperature"])
}
