// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusremotewriteexporter

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/prometheusremotewriteexporter/internal/metadata"
)

// TestWireBytes counts HTTP socket bytes and the snappy body for one
// PushMetrics call. The batch matches clickhouseexporter realisticMetrics.
func TestWireBytes(t *testing.T) {
	md, datapoints := realisticMetrics(20)

	var socketBytes atomic.Int64
	var bodyBytes atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		bodyBytes.Add(int64(len(body)))
		w.WriteHeader(http.StatusNoContent)
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := httptest.NewUnstartedServer(handler)
	require.NoError(t, server.Listener.Close())
	server.Listener = &countingListener{Listener: ln, n: &socketBytes}
	server.Start()
	t.Cleanup(server.Close)

	cfg := createDefaultConfig().(*Config)
	cfg.HTTP.Endpoint = server.URL
	cfg.RemoteWriteQueue.NumConsumers = 1

	set := exportertest.NewNopSettings(metadata.Type)
	prwe, err := newPRWExporter(cfg, set)
	require.NoError(t, err)
	require.NoError(t, prwe.Start(t.Context(), componenttest.NewNopHost()))
	t.Cleanup(func() {
		require.NoError(t, prwe.Shutdown(context.Background()))
	})

	require.NoError(t, prwe.PushMetrics(t.Context(), md))

	t.Logf("path\tsocket_bytes\tsnappy_body\tB/datapoint")
	t.Logf("prometheusremotewrite\t%d\t%d\t%.2f",
		socketBytes.Load(), bodyBytes.Load(), float64(socketBytes.Load())/float64(datapoints))
}

type countingListener struct {
	net.Listener
	n *atomic.Int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &countingServerConn{Conn: conn, n: l.n}, nil
}

type countingServerConn struct {
	net.Conn
	n *atomic.Int64
}

func (c *countingServerConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.n.Add(int64(n))
	}
	return n, err
}

// realisticMetrics builds one gauge, sum, histogram, summary, and exponential
// histogram per service. The returned count is the number of OTel datapoints.
func realisticMetrics(services int) (pmetric.Metrics, int) {
	md := pmetric.NewMetrics()
	now := time.Now().UTC().Truncate(time.Millisecond)
	for s := range services {
		rm := md.ResourceMetrics().AppendEmpty()
		res := rm.Resource().Attributes()
		res.PutStr("service.name", fmt.Sprintf("service-%d", s))
		res.PutStr("service.version", "1.2.3")
		res.PutStr("service.instance.id", fmt.Sprintf("instance-%d", s))
		res.PutStr("host.name", fmt.Sprintf("host-%d.example.com", s))
		res.PutStr("deployment.environment", "production")

		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName("bench-scope")
		sm.Scope().SetVersion("v1")

		gauge := sm.Metrics().AppendEmpty()
		gauge.SetName("bench.temperature")
		gauge.SetUnit("Cel")
		gaugeDP := gauge.SetEmptyGauge().DataPoints().AppendEmpty()
		gaugeDP.SetTimestamp(pcommon.NewTimestampFromTime(now))
		gaugeDP.SetDoubleValue(21.5)
		gaugeDP.Attributes().PutStr("room", "lab")
		gaugeDP.Attributes().PutStr("sensor", fmt.Sprintf("sensor-%d", s%4))

		sum := sm.Metrics().AppendEmpty()
		sum.SetName("bench.requests")
		sum.SetUnit("1")
		sumData := sum.SetEmptySum()
		sumData.SetIsMonotonic(true)
		sumData.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		sumDP := sumData.DataPoints().AppendEmpty()
		sumDP.SetTimestamp(pcommon.NewTimestampFromTime(now))
		sumDP.SetIntValue(int64(1000 + s))
		sumDP.Attributes().PutStr("method", "GET")

		hist := sm.Metrics().AppendEmpty()
		hist.SetName("bench.latency")
		hist.SetUnit("s")
		histData := hist.SetEmptyHistogram()
		histData.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		histDP := histData.DataPoints().AppendEmpty()
		histDP.SetTimestamp(pcommon.NewTimestampFromTime(now))
		histDP.SetCount(8)
		histDP.SetSum(1.2)
		histDP.ExplicitBounds().FromRaw([]float64{0.1, 0.5, 1})
		histDP.BucketCounts().FromRaw([]uint64{2, 3, 2, 1})
		histDP.Attributes().PutStr("route", "/api")

		summary := sm.Metrics().AppendEmpty()
		summary.SetName("bench.payload")
		summary.SetUnit("By")
		summaryDP := summary.SetEmptySummary().DataPoints().AppendEmpty()
		summaryDP.SetTimestamp(pcommon.NewTimestampFromTime(now))
		summaryDP.SetCount(4)
		summaryDP.SetSum(4000)
		q50 := summaryDP.QuantileValues().AppendEmpty()
		q50.SetQuantile(0.5)
		q50.SetValue(800)
		q99 := summaryDP.QuantileValues().AppendEmpty()
		q99.SetQuantile(0.99)
		q99.SetValue(1500)

		expHist := sm.Metrics().AppendEmpty()
		expHist.SetName("bench.exp.latency")
		expHist.SetUnit("s")
		expData := expHist.SetEmptyExponentialHistogram()
		expData.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		expDP := expData.DataPoints().AppendEmpty()
		expDP.SetTimestamp(pcommon.NewTimestampFromTime(now))
		expDP.SetScale(2)
		expDP.SetCount(2)
		expDP.SetSum(0.4)
		expDP.Positive().SetOffset(0)
		expDP.Positive().BucketCounts().FromRaw([]uint64{1, 1})
	}
	return md, services * 5
}
