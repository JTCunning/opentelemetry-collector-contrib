// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metrics // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal/metrics"

import (
	"context"
	"errors"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal/sqltemplates"
)

const (
	// SamplesColumn is the outer samples column of TimeSeries tables of schema
	// version 3 and later.
	SamplesColumn = "samples"
	// LegacySamplesColumn is the outer samples column of TimeSeries tables of
	// schema versions before 3.
	LegacySamplesColumn = "time_series"
	// MetricFamilyColumn is the outer column carrying the metric family name.
	MetricFamilyColumn = "metric_family"
	// LegacyMetricFamilyColumn is the metric family column name exposed by
	// servers before the rename to metric_family.
	LegacyMetricFamilyColumn = "metric_family_name"
)

// timeSeriesEngineSetting returns the server setting that enables the
// TimeSeries engine. ClickHouse 26.9 moved the engine to the private preview
// tier and renamed the setting; the old name is kept as an alias there, but the
// new name is unknown to older servers.
func timeSeriesEngineSetting(db driver.Conn) string {
	version, err := db.ServerVersion()
	if err != nil || version == nil {
		return "allow_experimental_time_series_table"
	}
	if version.Version.Major > 26 || (version.Version.Major == 26 && version.Version.Minor >= 9) {
		return "enable_time_series_table"
	}
	return "allow_experimental_time_series_table"
}

// NewTimeSeriesTable creates a TimeSeries engine table. The engine is gated
// behind a server setting, which is enabled for the DDL statement only.
func NewTimeSeriesTable(ctx context.Context, database, tableName, clusterStr string, db driver.Conn) error {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		timeSeriesEngineSetting(db): 1,
	}))

	query := fmt.Sprintf(sqltemplates.MetricsTimeSeriesCreateTable, database, tableName, clusterStr)
	if err := db.Exec(ctx, query); err != nil {
		return fmt.Errorf("exec create TimeSeries metrics table sql: %w", err)
	}

	return nil
}

// TimeSeriesModel converts OTel metrics to Prometheus-shaped samples and
// inserts them into a ClickHouse
// TimeSeries engine table through its outer insert columns:
// (metric_name, tags, samples) for samples and
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
}

// NewTimeSeriesModel creates a TimeSeriesModel writing to the given table.
// DetectSchema must be called before Insert.
func NewTimeSeriesModel(database, tableName string) *TimeSeriesModel {
	return &TimeSeriesModel{
		database:  database,
		tableName: tableName,
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
	case hasColumn(columnSet, SamplesColumn):
		samplesColumn = SamplesColumn
	case hasColumn(columnSet, LegacySamplesColumn):
		samplesColumn = LegacySamplesColumn
	case hasColumn(columnSet, "timestamp") && hasColumn(columnSet, "value"):
		return fmt.Errorf("table %q.%q has the pre-26.6 TimeSeries column layout; direct INSERT into TimeSeries tables requires ClickHouse 26.6 or newer", m.database, m.tableName)
	default:
		return fmt.Errorf("table %q.%q does not look like a TimeSeries table: no %s or %s column found", m.database, m.tableName, SamplesColumn, LegacySamplesColumn)
	}

	metricFamilyColumn := ""
	switch {
	case hasColumn(columnSet, MetricFamilyColumn):
		metricFamilyColumn = MetricFamilyColumn
	case hasColumn(columnSet, LegacyMetricFamilyColumn):
		metricFamilyColumn = LegacyMetricFamilyColumn
	default:
		logger.Warn("TimeSeries table has no metric metadata columns; skipping metadata inserts",
			zap.String("database", m.database), zap.String("table", m.tableName))
	}

	m.SetColumns(samplesColumn, metricFamilyColumn)
	return nil
}

// SetColumns prepares the INSERT statements for the given outer column names.
// An empty metricFamilyColumn disables metadata inserts. DetectSchema calls
// this with the names it finds on the server; callers without a live
// connection (tests, benchmarks) call it directly.
func (m *TimeSeriesModel) SetColumns(samplesColumn, metricFamilyColumn string) {
	m.insertSQL = fmt.Sprintf(sqltemplates.MetricsTimeSeriesInsert, m.database, m.tableName, samplesColumn)
	if metricFamilyColumn == "" {
		m.metaInsertSQL = ""
		return
	}
	m.metaInsertSQL = fmt.Sprintf(sqltemplates.MetricsTimeSeriesMetaInsert, m.database, m.tableName, metricFamilyColumn)
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
	if m.insertSQL == "" {
		return errors.New("TimeSeries model not initialized: DetectSchema was not called")
	}

	rows, metaRows, droppedExp, droppedDelta, convErr := convertToTimeSeries(md)
	if convErr != nil {
		logger.Warn("dropped metrics not translatable to the Prometheus data model", zap.Error(convErr))
	}
	if droppedDelta > 0 {
		logger.Warn("dropped metrics with delta temporality; use the deltatocumulative processor upstream to keep them",
			zap.Int("metrics", droppedDelta))
	}
	if droppedExp > 0 {
		logger.Warn("dropped exponential histograms: the TimeSeries engine only stores float samples; convert them upstream (e.g. transform processor) to keep them",
			zap.Int("datapoints", droppedExp))
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
