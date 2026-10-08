// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metrics // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal/metrics"

import (
	"errors"
	"maps"
	"math"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/prometheus/otlptranslator"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const (
	attrServiceName       = "service.name"
	attrServiceNamespace  = "service.namespace"
	attrServiceInstanceID = "service.instance.id"
	labelJob              = "job"
	labelInstance         = "instance"
	suffixSum             = "_sum"
	suffixCount           = "_count"
	suffixBucket          = "_bucket"
	labelLe               = "le"
	labelQuantile         = "quantile"
	lePositiveInf         = "+Inf"
)

// staleNaN is the Prometheus stale marker.
var staleNaN = math.Float64frombits(0x7ff0000000000002)

type tsConverter struct {
	metricNamer otlptranslator.MetricNamer
	labelNamer  otlptranslator.LabelNamer
	unitNamer   otlptranslator.UnitNamer

	series map[string]*timeSeriesRow
	order  []string

	meta     []metadataRow
	seenMeta map[string]struct{}

	droppedExp   int
	droppedDelta int
	err          error
}

func newTSConverter() *tsConverter {
	return &tsConverter{
		metricNamer: otlptranslator.MetricNamer{WithMetricSuffixes: true},
		//nolint:staticcheck // SA1019 UnderscoreLabelSanitization matches the Prometheus label rules this exporter follows.
		labelNamer: otlptranslator.LabelNamer{UnderscoreLabelSanitization: true},
		unitNamer:  otlptranslator.UnitNamer{},
		series:     map[string]*timeSeriesRow{},
		seenMeta:   map[string]struct{}{},
	}
}

// convertToTimeSeries walks OTel metrics and returns Prometheus-shaped samples
// plus metric-family metadata. Exponential histograms and delta temporality
// are counted and omitted.
func convertToTimeSeries(md pmetric.Metrics) (rows []timeSeriesRow, meta []metadataRow, droppedExp, droppedDelta int, err error) {
	c := newTSConverter()
	c.convert(md)
	rows = make([]timeSeriesRow, 0, len(c.order))
	for _, key := range c.order {
		rows = append(rows, *c.series[key])
	}
	return rows, c.meta, c.droppedExp, c.droppedDelta, c.err
}

func (c *tsConverter) convert(md pmetric.Metrics) {
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		resource := rm.Resource()
		var latest pcommon.Timestamp
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)
			scope := sm.Scope()
			for k := 0; k < sm.Metrics().Len(); k++ {
				metric := sm.Metrics().At(k)
				latest = max(latest, latestTimestamp(metric))
				c.addMetric(resource, scope, metric)
			}
		}
		c.addTargetInfo(resource, latest)
	}
}

func (c *tsConverter) addMetric(resource pcommon.Resource, scope pcommon.InstrumentationScope, metric pmetric.Metric) {
	if !cumulativeTemporality(metric) {
		c.droppedDelta++
		return
	}
	if metric.Type() == pmetric.MetricTypeExponentialHistogram {
		if metric.ExponentialHistogram().DataPoints().Len() > 0 {
			c.droppedExp += metric.ExponentialHistogram().DataPoints().Len()
		}
		return
	}

	promMetric, metaType, ok := prometheusMetric(metric)
	if !ok {
		return
	}
	name, err := c.metricNamer.Build(promMetric)
	if err != nil {
		c.err = errors.Join(c.err, err)
		return
	}
	if name == "" {
		return
	}

	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		c.addNumberPoints(resource, scope, metric.Gauge().DataPoints(), name)
	case pmetric.MetricTypeSum:
		c.addNumberPoints(resource, scope, metric.Sum().DataPoints(), name)
	case pmetric.MetricTypeHistogram:
		c.addHistogramPoints(resource, scope, metric.Histogram().DataPoints(), name)
	case pmetric.MetricTypeSummary:
		c.addSummaryPoints(resource, scope, metric.Summary().DataPoints(), name)
	default:
		return
	}

	if _, seen := c.seenMeta[name]; seen {
		return
	}
	c.seenMeta[name] = struct{}{}
	c.meta = append(c.meta, metadataRow{
		metricFamily: name,
		metricType:   metaType,
		unit:         c.unitNamer.Build(metric.Unit()),
		help:         metric.Description(),
	})
}

func prometheusMetric(metric pmetric.Metric) (otlptranslator.Metric, string, bool) {
	out := otlptranslator.Metric{Name: metric.Name(), Unit: metric.Unit()}
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		out.Type = otlptranslator.MetricTypeGauge
		return out, "gauge", true
	case pmetric.MetricTypeSum:
		if metric.Sum().IsMonotonic() {
			out.Type = otlptranslator.MetricTypeMonotonicCounter
			return out, "counter", true
		}
		out.Type = otlptranslator.MetricTypeGauge
		return out, "gauge", true
	case pmetric.MetricTypeHistogram:
		out.Type = otlptranslator.MetricTypeHistogram
		return out, "histogram", true
	case pmetric.MetricTypeSummary:
		out.Type = otlptranslator.MetricTypeSummary
		return out, "summary", true
	default:
		return out, "", false
	}
}

func cumulativeTemporality(metric pmetric.Metric) bool {
	switch metric.Type() {
	case pmetric.MetricTypeGauge, pmetric.MetricTypeSummary:
		return true
	case pmetric.MetricTypeSum:
		return metric.Sum().AggregationTemporality() == pmetric.AggregationTemporalityCumulative
	case pmetric.MetricTypeHistogram:
		return metric.Histogram().AggregationTemporality() == pmetric.AggregationTemporalityCumulative
	case pmetric.MetricTypeExponentialHistogram:
		return metric.ExponentialHistogram().AggregationTemporality() == pmetric.AggregationTemporalityCumulative
	default:
		return false
	}
}

func (c *tsConverter) addNumberPoints(resource pcommon.Resource, scope pcommon.InstrumentationScope, points pmetric.NumberDataPointSlice, name string) {
	for i := 0; i < points.Len(); i++ {
		pt := points.At(i)
		c.addSample(name, c.seriesTags(resource, pt.Attributes(), scope), millisTime(pt.Timestamp()), numberSample(pt))
	}
}

func numberSample(pt pmetric.NumberDataPoint) float64 {
	if pt.Flags().NoRecordedValue() {
		return staleNaN
	}
	if pt.ValueType() == pmetric.NumberDataPointValueTypeInt {
		return float64(pt.IntValue())
	}
	return pt.DoubleValue()
}

func (c *tsConverter) addHistogramPoints(resource pcommon.Resource, scope pcommon.InstrumentationScope, points pmetric.HistogramDataPointSlice, name string) {
	for i := 0; i < points.Len(); i++ {
		pt := points.At(i)
		ts := millisTime(pt.Timestamp())
		base := c.seriesTags(resource, pt.Attributes(), scope)
		stale := pt.Flags().NoRecordedValue()

		if pt.HasSum() {
			c.addSample(name+suffixSum, base, ts, staleOr(stale, pt.Sum()))
		}
		c.addSample(name+suffixCount, base, ts, staleOr(stale, float64(pt.Count())))

		var cumulative uint64
		bounds := pt.ExplicitBounds()
		buckets := pt.BucketCounts()
		for b := 0; b < bounds.Len() && b < buckets.Len(); b++ {
			cumulative += buckets.At(b)
			tags := withLabel(base, labelLe, strconv.FormatFloat(bounds.At(b), 'f', -1, 64))
			c.addSample(name+suffixBucket, tags, ts, staleOr(stale, float64(cumulative)))
		}
		c.addSample(name+suffixBucket, withLabel(base, labelLe, lePositiveInf), ts, staleOr(stale, float64(pt.Count())))
	}
}

func (c *tsConverter) addSummaryPoints(resource pcommon.Resource, scope pcommon.InstrumentationScope, points pmetric.SummaryDataPointSlice, name string) {
	for i := 0; i < points.Len(); i++ {
		pt := points.At(i)
		ts := millisTime(pt.Timestamp())
		base := c.seriesTags(resource, pt.Attributes(), scope)
		stale := pt.Flags().NoRecordedValue()
		c.addSample(name+suffixSum, base, ts, staleOr(stale, pt.Sum()))
		c.addSample(name+suffixCount, base, ts, staleOr(stale, float64(pt.Count())))
		for q := 0; q < pt.QuantileValues().Len(); q++ {
			qt := pt.QuantileValues().At(q)
			tags := withLabel(base, labelQuantile, strconv.FormatFloat(qt.Quantile(), 'f', -1, 64))
			c.addSample(name, tags, ts, staleOr(stale, qt.Value()))
		}
	}
}

func (c *tsConverter) addTargetInfo(resource pcommon.Resource, ts pcommon.Timestamp) {
	if ts == 0 || resource.Attributes().Len() == 0 {
		return
	}
	identifying := []string{attrServiceNamespace, attrServiceName, attrServiceInstanceID}
	nonIdentifying := resource.Attributes().Len()
	for _, key := range identifying {
		if _, ok := resource.Attributes().Get(key); ok {
			nonIdentifying--
		}
	}
	if nonIdentifying == 0 {
		return
	}
	tags := c.seriesTags(resource, resource.Attributes(), pcommon.NewInstrumentationScope(), identifying...)
	if _, ok := tags[labelJob]; !ok {
		if _, ok := tags[labelInstance]; !ok {
			return
		}
	}
	c.addSample(otlptranslator.TargetInfoMetricName, tags, millisTime(ts), 1)
}

func (c *tsConverter) seriesTags(resource pcommon.Resource, attrs pcommon.Map, scope pcommon.InstrumentationScope, ignore ...string) map[string]string {
	type pair struct {
		key   string
		value string
	}
	pairs := make([]pair, 0, attrs.Len())
	attrs.Range(func(key string, value pcommon.Value) bool {
		if slices.Contains(ignore, key) {
			return true
		}
		str := value.AsString()
		if str == "" {
			return true
		}
		pairs = append(pairs, pair{key: key, value: str})
		return true
	})
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].key < pairs[j].key })

	tags := make(map[string]string, len(pairs)+4)
	for _, p := range pairs {
		sanitized, err := c.labelNamer.Build(p.key)
		if err != nil {
			c.err = errors.Join(c.err, err)
			continue
		}
		if existing, ok := tags[sanitized]; ok && existing != p.value {
			tags[sanitized] = existing + ";" + p.value
			continue
		}
		tags[sanitized] = p.value
	}

	if service, ok := resource.Attributes().Get(attrServiceName); ok && service.AsString() != "" {
		job := service.AsString()
		if namespace, hasNS := resource.Attributes().Get(attrServiceNamespace); hasNS && namespace.AsString() != "" {
			job = namespace.AsString() + "/" + job
		}
		tags[labelJob] = job
	}
	if instance, ok := resource.Attributes().Get(attrServiceInstanceID); ok && instance.AsString() != "" {
		tags[labelInstance] = instance.AsString()
	}
	if scope.Name() != "" {
		tags[otlptranslator.ScopeNameLabelKey] = scope.Name()
	}
	if scope.Version() != "" {
		tags[otlptranslator.ScopeVersionLabelKey] = scope.Version()
	}
	return tags
}

func (c *tsConverter) addSample(name string, tags map[string]string, ts time.Time, value float64) {
	if name == "" {
		return
	}
	key := seriesKey(name, tags)
	row := c.series[key]
	if row == nil {
		row = &timeSeriesRow{
			metricName: name,
			tags:       tags,
			samples:    make([][]any, 0, 1),
		}
		c.series[key] = row
		c.order = append(c.order, key)
	}
	row.samples = append(row.samples, []any{ts, value})
}

func withLabel(tags map[string]string, key, value string) map[string]string {
	out := maps.Clone(tags)
	out[key] = value
	return out
}

func seriesKey(name string, tags map[string]string) string {
	keys := make([]string, 0, len(tags))
	for key := range tags {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	size := len(name)
	for _, key := range keys {
		size += 2 + len(key) + len(tags[key])
	}
	buf := make([]byte, 0, size)
	buf = append(buf, name...)
	for _, key := range keys {
		buf = append(buf, 0)
		buf = append(buf, key...)
		buf = append(buf, 0)
		buf = append(buf, tags[key]...)
	}
	return string(buf)
}

func staleOr(stale bool, value float64) float64 {
	if stale {
		return staleNaN
	}
	return value
}

func millisTime(ts pcommon.Timestamp) time.Time {
	return time.UnixMilli(ts.AsTime().UnixNano() / int64(time.Millisecond)).UTC()
}

func latestTimestamp(metric pmetric.Metric) pcommon.Timestamp {
	var ts pcommon.Timestamp
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		pts := metric.Gauge().DataPoints()
		for i := 0; i < pts.Len(); i++ {
			ts = max(ts, pts.At(i).Timestamp())
		}
	case pmetric.MetricTypeSum:
		pts := metric.Sum().DataPoints()
		for i := 0; i < pts.Len(); i++ {
			ts = max(ts, pts.At(i).Timestamp())
		}
	case pmetric.MetricTypeHistogram:
		pts := metric.Histogram().DataPoints()
		for i := 0; i < pts.Len(); i++ {
			ts = max(ts, pts.At(i).Timestamp())
		}
	case pmetric.MetricTypeExponentialHistogram:
		pts := metric.ExponentialHistogram().DataPoints()
		for i := 0; i < pts.Len(); i++ {
			ts = max(ts, pts.At(i).Timestamp())
		}
	case pmetric.MetricTypeSummary:
		pts := metric.Summary().DataPoints()
		for i := 0; i < pts.Len(); i++ {
			ts = max(ts, pts.At(i).Timestamp())
		}
	}
	return ts
}
