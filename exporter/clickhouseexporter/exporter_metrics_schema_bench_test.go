// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package clickhouseexporter

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// TestMetricsSchemaBenchmark pushes the same batches through the otel and
// timeseries schemas and logs the collector's wall time, CPU, allocations,
// and RSS. It does not compare ClickHouse storage.
func TestMetricsSchemaBenchmark(t *testing.T) {
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
	const batches = 20
	totalPoints := datapoints * batches

	t.Logf("schema\tpoints/s\tcpu_ms/10k\tB/point\tmallocs/point\twall_s\tpeak_rss_kib")
	for _, schema := range []string{schemaOTel, schemaTimeSeries} {
		cfg := withDefaultConfig(func(cfg *Config) {
			cfg.Endpoint = endpoint
			cfg.Database = "otel_bench_" + schema
			cfg.MetricsSchema = schema
			cfg.MetricsTimeSeriesTableName = "otel_metrics"
		})
		require.NoError(t, cfg.Validate())
		exp := newMetricsExporter(zaptest.NewLogger(t), cfg)
		require.NoError(t, exp.start(t.Context(), nil))

		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		var ruBefore syscall.Rusage
		require.NoError(t, syscall.Getrusage(syscall.RUSAGE_SELF, &ruBefore))
		rss := readVMRSS()

		start := time.Now()
		for range batches {
			require.NoError(t, exp.pushMetricsData(t.Context(), md))
			if current := readVMRSS(); current > rss {
				rss = current
			}
		}
		wall := time.Since(start)

		var ruAfter syscall.Rusage
		require.NoError(t, syscall.Getrusage(syscall.RUSAGE_SELF, &ruAfter))
		runtime.ReadMemStats(&after)
		require.NoError(t, exp.shutdown(t.Context()))

		cpu := rusageDuration(ruAfter) - rusageDuration(ruBefore)
		allocBytes := after.TotalAlloc - before.TotalAlloc
		mallocs := after.Mallocs - before.Mallocs
		pointsPerSec := float64(totalPoints) / wall.Seconds()
		cpuPer10k := cpu.Seconds() * 1000 * 10000 / float64(totalPoints)
		bytesPerPoint := float64(allocBytes) / float64(totalPoints)
		mallocsPerPoint := float64(mallocs) / float64(totalPoints)

		t.Logf("%s\t%.0f\t%.2f\t%.0f\t%.1f\t%.3f\t%d",
			schema, pointsPerSec, cpuPer10k, bytesPerPoint, mallocsPerPoint, wall.Seconds(), rss)
	}
}

func rusageDuration(ru syscall.Rusage) time.Duration {
	user := time.Duration(ru.Utime.Sec)*time.Second + time.Duration(ru.Utime.Usec)*time.Microsecond
	sys := time.Duration(ru.Stime.Sec)*time.Second + time.Duration(ru.Stime.Usec)*time.Microsecond
	return user + sys
}

func readVMRSS() uint64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		kb, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0
		}
		return kb
	}
	return 0
}
