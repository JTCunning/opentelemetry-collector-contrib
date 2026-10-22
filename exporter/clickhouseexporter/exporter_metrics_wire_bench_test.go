// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package clickhouseexporter

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap/zaptest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal/metrics"
)

// TestMetricsWireBytes counts native-protocol socket writes for two pushes of
// realisticMetrics. The counter starts after schema creation and resets before
// the second push.
func TestMetricsWireBytes(t *testing.T) {
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
	md, datapoints := realisticMetrics(20)

	t.Logf("schema\tcompress\tpush\tsocket_bytes\tB/datapoint")
	for _, schema := range []string{schemaOTel, schemaTimeSeries} {
		for _, compress := range []string{"lz4", "none"} {
			first, second := measureMetricsWireBytes(t, endpoint, schema, compress, md)
			t.Logf("%s\t%s\t1\t%d\t%.2f", schema, compress, first, float64(first)/float64(datapoints))
			t.Logf("%s\t%s\t2\t%d\t%.2f", schema, compress, second, float64(second)/float64(datapoints))
			if schema == schemaTimeSeries && compress == "lz4" {
				require.Less(t, first, int64(6200))
				require.Greater(t, first-second, int64(400))
			}
		}
	}
}

func measureMetricsWireBytes(t *testing.T, endpoint, schema, compress string, md pmetric.Metrics) (int64, int64) {
	t.Helper()

	var written atomic.Int64
	dialer := &net.Dialer{Timeout: 30 * time.Second}
	dial := func(ctx context.Context, addr string) (net.Conn, error) {
		raw, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}
		return &countingConn{Conn: raw, n: &written}, nil
	}

	cfg := withDefaultConfig(func(cfg *Config) {
		cfg.Endpoint = endpoint
		cfg.Database = fmt.Sprintf("otel_wire_%s_%s", schema, compress)
		cfg.MetricsSchema = schema
		cfg.MetricsTimeSeriesTableName = "otel_metrics"
		cfg.Compress = compress
	})
	require.NoError(t, cfg.Validate())

	exp := newMetricsExporter(zaptest.NewLogger(t), cfg)
	require.NoError(t, startWithDial(t.Context(), exp, dial))
	t.Cleanup(func() {
		require.NoError(t, exp.shutdown(context.Background()))
	})

	written.Store(0)
	require.NoError(t, exp.pushMetricsData(t.Context(), md))
	first := written.Load()
	written.Store(0)
	require.NoError(t, exp.pushMetricsData(t.Context(), md))
	return first, written.Load()
}

func startWithDial(ctx context.Context, exp *metricsExporter, dial func(context.Context, string) (net.Conn, error)) error {
	metrics.SetLogger(exp.logger)

	opt, err := exp.cfg.buildClickHouseOptions()
	if err != nil {
		return err
	}
	opt.DialContext = dial

	exp.db, err = internal.NewClickhouseClientFromOptions(opt, exp.cfg.shouldCreateSchema())
	if err != nil {
		return err
	}

	if exp.cfg.metricsSchema() == schemaTimeSeries {
		exp.tsModel = metrics.NewTimeSeriesModel(exp.cfg.database(), exp.cfg.MetricsTimeSeriesTableName)
	}

	if exp.cfg.shouldCreateSchema() {
		database := exp.cfg.database()
		clusterStr := exp.cfg.clusterString()
		if err := internal.CreateDatabase(ctx, exp.db, database, clusterStr); err != nil {
			return err
		}

		if exp.tsModel != nil {
			if err := metrics.NewTimeSeriesTable(ctx, database, exp.cfg.MetricsTimeSeriesTableName, clusterStr, exp.db); err != nil {
				return err
			}
		} else {
			ttlExpr := internal.GenerateTTLExpr(exp.cfg.TTL, "toDateTime(TimeUnix)")
			if err := metrics.NewMetricsTable(ctx, exp.tablesConfig, database, clusterStr, exp.cfg.tableEngineString(), ttlExpr, exp.db); err != nil {
				return err
			}
		}
	}

	if exp.tsModel != nil {
		if err := exp.tsModel.DetectSchema(ctx, exp.db); err != nil {
			return err
		}
	}

	return nil
}

type countingConn struct {
	net.Conn
	n *atomic.Int64
}

func (c *countingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if n > 0 {
		c.n.Add(int64(n))
	}
	return n, err
}
