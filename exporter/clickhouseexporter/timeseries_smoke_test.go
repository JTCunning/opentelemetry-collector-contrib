// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build timeseries_smoke

// Temporary live smoke test for the TimeSeries metrics schema. Run with:
//
//	go test -count=1 -tags timeseries_smoke -run TestTimeSeriesSchemaSmoke -v .
//
// Requires a ClickHouse server with TimeSeries engine support on
// CLICKHOUSE_TS_SMOKE_ENDPOINT (default tcp://127.0.0.1:19000).
package clickhouseexporter

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestTimeSeriesSchemaSmoke(t *testing.T) {
	endpoint := os.Getenv("CLICKHOUSE_TS_SMOKE_ENDPOINT")
	if endpoint == "" {
		endpoint = "tcp://127.0.0.1:19000"
	}

	ctx := context.Background()

	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = endpoint
	cfg.Database = "otel_ts_smoke"
	cfg.MetricsSchema = "timeseries"
	cfg.MetricsTimeSeriesTableName = "otel_metrics"

	require.NoError(t, cfg.Validate())

	exp := newMetricsExporter(exportertest.NewNopSettings(exportertest.NopType).Logger, cfg)
	require.NoError(t, exp.start(ctx, nil))
	defer func() { require.NoError(t, exp.shutdown(ctx)) }()

	// Fixture: gauge + monotonic sum + classic histogram + metadata.
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

	require.NoError(t, exp.pushMetricsData(ctx, md))

	// Read back through the TimeSeries table functions.
	opts, err := clickhouse.ParseDSN(endpoint + "/" + cfg.Database)
	require.NoError(t, err)
	conn, err := clickhouse.Open(opts)
	require.NoError(t, err)
	defer conn.Close()

	var tagRows []struct {
		MetricName string            `ch:"metric_name"`
		Tags       map[string]string `ch:"tags"`
	}
	require.NoError(t, conn.Select(ctx,
		&tagRows,
		"SELECT metric_name, tags FROM timeSeriesTags(otel_ts_smoke.otel_metrics) ORDER BY metric_name",
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
	row := conn.QueryRow(ctx, "SELECT count() FROM timeSeriesData(otel_ts_smoke.otel_metrics)")
	require.NoError(t, row.Scan(&sampleCount))
	// gauge(1) + sum(1) + histogram bucket(3)+sum(1)+count(1) + target_info(1) = 8
	assert.Equal(t, uint64(8), sampleCount, "expected 8 samples")

	var metaRows []struct {
		Family string `ch:"metric_family_name"`
		Type   string `ch:"type"`
		Unit   string `ch:"unit"`
		Help   string `ch:"help"`
	}
	require.NoError(t, conn.Select(ctx,
		&metaRows,
		"SELECT metric_family_name, type, unit, help FROM timeSeriesMetrics(otel_ts_smoke.otel_metrics) ORDER BY metric_family_name",
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
