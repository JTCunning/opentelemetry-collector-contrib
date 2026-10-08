// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metrics // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal/metrics"

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/prometheus/prompb"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal/sqltemplates"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheusremotewrite"
)

// metricNameLabel is the reserved Prometheus label carrying the metric name.
const metricNameLabel = "__name__"

// NewTimeSeriesTable creates a TimeSeries engine table. The engine is
// experimental, so the DDL runs with allow_experimental_time_series_table
// enabled for that statement only.
func NewTimeSeriesTable(ctx context.Context, database, tableName, clusterStr string, db driver.Conn) error {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"allow_experimental_time_series_table": 1,
	}))

	query := fmt.Sprintf(sqltemplates.MetricsTimeSeriesCreateTable, database, tableName, clusterStr)
	if err := db.Exec(ctx, query); err != nil {
		return fmt.Errorf("exec create TimeSeries metrics table sql: %w", err)
	}

	return nil
}

// TimeSeriesModel converts OTel metrics to the Prometheus data model (via the
// prometheusremotewrite translator) and inserts them into a ClickHouse
// TimeSeries engine table through its outer insert columns:
// (metric_name, tags, samples|time_series) for samples and
// (metric_family, type, unit, help) for metric metadata.
//
// Direct INSERT into TimeSeries tables requires ClickHouse 26.6+. Older
// versions reject INSERT entirely and expose a different outer column layout;
// DetectSchema turns that into a clear startup error.
type TimeSeriesModel struct {
	database  string
	tableName string

	insertSQL     string
	metaInsertSQL string

	translatorSettings prometheusremotewrite.Settings
}

// NewTimeSeriesModel creates a TimeSeriesModel writing to the given table.
// DetectSchema must be called before Insert.
func NewTimeSeriesModel(database, tableName string) *TimeSeriesModel {
	return &TimeSeriesModel{
		database:  database,
		tableName: tableName,
		translatorSettings: prometheusremotewrite.Settings{
			AddMetricSuffixes: true,
		},
	}
}

// DetectSchema inspects the target table's outer columns and prepares the
// matching INSERT statements.
func (m *TimeSeriesModel) DetectSchema(ctx context.Context, db driver.Conn) error {
	columns, err := internal.GetTableColumns(ctx, db, m.database, m.tableName)
	if err != nil {
		return fmt.Errorf("detect TimeSeries table schema: %w", err)
	}

	columnSet := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		columnSet[column] = struct{}{}
	}

	samplesColumn := ""
	switch {
	case hasColumn(columnSet, "samples"):
		samplesColumn = "samples"
	case hasColumn(columnSet, "time_series"):
		samplesColumn = "time_series"
	case hasColumn(columnSet, "timestamp") && hasColumn(columnSet, "value"):
		return fmt.Errorf("table %q.%q has the pre-26.6 TimeSeries column layout; direct INSERT into TimeSeries tables requires ClickHouse 26.6 or newer", m.database, m.tableName)
	default:
		return fmt.Errorf("table %q.%q does not look like a TimeSeries table: no samples or time_series column found", m.database, m.tableName)
	}
	m.insertSQL = fmt.Sprintf(sqltemplates.MetricsTimeSeriesInsert, m.database, m.tableName, samplesColumn)

	switch {
	case hasColumn(columnSet, "metric_family"):
		m.metaInsertSQL = fmt.Sprintf(sqltemplates.MetricsTimeSeriesMetaInsert, m.database, m.tableName, "metric_family")
	case hasColumn(columnSet, "metric_family_name"):
		m.metaInsertSQL = fmt.Sprintf(sqltemplates.MetricsTimeSeriesMetaInsert, m.database, m.tableName, "metric_family_name")
	default:
		m.metaInsertSQL = ""
		logger.Warn("TimeSeries table has no metric metadata columns; skipping metadata inserts",
			zap.String("database", m.database), zap.String("table", m.tableName))
	}

	return nil
}

func hasColumn(columnSet map[string]struct{}, name string) bool {
	_, ok := columnSet[name]
	return ok
}

// timeSeriesRow is one row of the TimeSeries table's samples/tags insert.
type timeSeriesRow struct {
	metricName string
	tags       map[string]string
	// samples is the value for the outer Array(Tuple(DateTime64(3), Float64)) column.
	samples [][]any
}

// metadataRow is one row of the TimeSeries table's metrics-metadata insert.
type metadataRow struct {
	metricFamily string
	metricType   string
	unit         string
	help         string
}

// Insert translates the given metrics and writes them with two batches: one
// for samples+tags, one for metric metadata.
func (m *TimeSeriesModel) Insert(ctx context.Context, db driver.Conn, md pmetric.Metrics) error {
	tsMap, err := prometheusremotewrite.FromMetrics(md, m.translatorSettings)
	if err != nil {
		// The translator still returns the successfully converted series
		// alongside per-metric errors (e.g. dropped delta-temporality
		// datapoints). Those drops are not retryable, so log and continue.
		logger.Warn("dropped metrics not translatable to the Prometheus data model", zap.Error(err))
	}

	rows, droppedNativeHistograms := toTimeSeriesRows(tsMap)
	if droppedNativeHistograms > 0 {
		logger.Warn("dropped native-histogram series: the TimeSeries engine only stores float samples; convert exponential histograms upstream (e.g. transform processor) to keep them",
			zap.Int("series", droppedNativeHistograms))
	}

	if m.insertSQL == "" {
		return errors.New("TimeSeries model not initialized: DetectSchema was not called")
	}

	if len(rows) > 0 {
		batch, batchErr := db.PrepareBatch(ctx, m.insertSQL)
		if batchErr != nil {
			return fmt.Errorf("prepare TimeSeries samples batch: %w", batchErr)
		}
		for _, row := range rows {
			if appendErr := batch.Append(row.metricName, row.tags, row.samples); appendErr != nil {
				return fmt.Errorf("append TimeSeries samples row: %w", appendErr)
			}
		}
		if sendErr := batch.Send(); sendErr != nil {
			return fmt.Errorf("send TimeSeries samples batch: %w", sendErr)
		}
	}

	metaRows := toMetadataRows(md, m.translatorSettings)
	if len(metaRows) > 0 && m.metaInsertSQL != "" {
		batch, batchErr := db.PrepareBatch(ctx, m.metaInsertSQL)
		if batchErr != nil {
			return fmt.Errorf("prepare TimeSeries metadata batch: %w", batchErr)
		}
		for _, row := range metaRows {
			if appendErr := batch.Append(row.metricFamily, row.metricType, row.unit, row.help); appendErr != nil {
				return fmt.Errorf("append TimeSeries metadata row: %w", appendErr)
			}
		}
		if sendErr := batch.Send(); sendErr != nil {
			return fmt.Errorf("send TimeSeries metadata batch: %w", sendErr)
		}
	}

	return nil
}

// toTimeSeriesRows converts translated Prometheus series into insert rows.
// Series carrying only native histograms (from OTel exponential histograms)
// cannot be represented as float samples and are counted as dropped.
func toTimeSeriesRows(tsMap map[string]*prompb.TimeSeries) (rows []timeSeriesRow, droppedNativeHistograms int) {
	rows = make([]timeSeriesRow, 0, len(tsMap))
	for _, ts := range tsMap {
		if len(ts.Histograms) > 0 {
			droppedNativeHistograms++
		}
		if len(ts.Samples) == 0 {
			continue
		}

		var metricName string
		tags := make(map[string]string, len(ts.Labels))
		for _, label := range ts.Labels {
			if label.Name == metricNameLabel {
				metricName = label.Value
				continue
			}
			tags[label.Name] = label.Value
		}
		if metricName == "" {
			// The TimeSeries engine requires a metric identity on every row.
			continue
		}

		samples := make([][]any, 0, len(ts.Samples))
		for _, sample := range ts.Samples {
			samples = append(samples, []any{time.UnixMilli(sample.Timestamp).UTC(), sample.Value})
		}

		rows = append(rows, timeSeriesRow{
			metricName: metricName,
			tags:       tags,
			samples:    samples,
		})
	}

	return rows, droppedNativeHistograms
}

// toMetadataRows builds deduplicated metric-family metadata rows. The type
// strings match what ClickHouse's own remote-write handler stores (lowercased
// Prometheus metadata type names).
func toMetadataRows(md pmetric.Metrics, settings prometheusremotewrite.Settings) []metadataRow {
	metadata, err := prometheusremotewrite.OtelMetricsToMetadata(md, settings)
	if err != nil {
		logger.Warn("failed to convert some metric metadata", zap.Error(err))
	}

	rows := make([]metadataRow, 0, len(metadata))
	seen := make(map[string]struct{}, len(metadata))
	for _, meta := range metadata {
		if meta == nil || meta.MetricFamilyName == "" {
			continue
		}
		if _, ok := seen[meta.MetricFamilyName]; ok {
			continue
		}
		seen[meta.MetricFamilyName] = struct{}{}

		rows = append(rows, metadataRow{
			metricFamily: meta.MetricFamilyName,
			metricType:   strings.ToLower(meta.Type.String()),
			unit:         meta.Unit,
			help:         meta.Help,
		})
	}

	return rows
}
