package report

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/germangorelkin/sql2csv"
	"github.com/xuri/excelize/v2"
	"go.uber.org/zap"
)

const (
	EXCEL_LIMIT_ROWS = 1_048_576
)

type XLSXReport struct {
	DB Repository

	logger *zap.SugaredLogger
}

type XLSXReportConfig struct {
	DB     Repository
	Logger *zap.SugaredLogger
}

func NewXLSXReport(cfg XLSXReportConfig) XLSXReport {
	return XLSXReport{
		DB:     cfg.DB,
		logger: cfg.Logger,
	}
}

func (srv XLSXReport) FileExtension() string {
	return ".xlsx"
}

func (srv XLSXReport) Build(ctx context.Context, cfg ReportConfig) error {
	return srv.exportData(ctx, cfg.FilePath, cfg.SQLQuery, cfg.ExcelConfig)
}

func (srv XLSXReport) createPivot(ctx context.Context, filePath string, wr *ExcelWriter) error {
	file, err := excelize.OpenFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to OpenFile %s:%w", filePath, err)
	}

	// if err := autofit(file, "data"); err != nil {
	// 	return fmt.Errorf("failed to autofit:%w", err)
	// }

	_ = file.NewSheet("pivot")
	dataRange := fmt.Sprintf("data!%s:%s", coordinatesToCellName(1, 1, true), coordinatesToCellName(len(wr.columns), wr.rowsCount, true))

	pivotDatas := make([]excelize.PivotTableField, 0, 1)
	for i := wr.columnsBeginData; i < len(wr.columns); i++ {
		pivotDatas = append(pivotDatas, excelize.PivotTableField{
			Data:     wr.columns[i],
			Name:     wr.columns[i],
			Subtotal: "Sum",
		})
	}

	if err := file.AddPivotTable(&excelize.PivotTableOption{
		DataRange:         dataRange,
		PivotTableRange:   "pivot!$A$2:$B$2",
		Rows:              []excelize.PivotTableField{{Data: "Сеть"}},
		Filter:            []excelize.PivotTableField{{Data: "Показатель"}},
		Data:              pivotDatas,
		RowGrandTotals:    true,
		UseAutoFormatting: true,
	}); err != nil {
		return fmt.Errorf("failed to AddPivotTable:%w", err)
	}

	if err := file.SaveAs(filePath); err != nil {
		return fmt.Errorf("failed to SaveAs:%w", err)
	}

	return nil
}

func (srv XLSXReport) exportData(ctx context.Context, filePath string, sqlQuery string, excelCfg ExcelConfig) error {
	file := excelize.NewFile()
	file.SetSheetName("Sheet1", "data")
	streamWriter, err := file.NewStreamWriter("data")
	if err != nil {
		return fmt.Errorf("failed to NewStreamWriter:%w", err)
	}

	rd := sql2csv.SQLReader{DB: srv.DB.GetDB(), Columns: true}

	wr := NewExcelWriter(ExcelWriterConfig{
		StreamWriter: streamWriter,
		Logger:       srv.logger.Named("excel-writer"),
	})

	err = rd.Read(ctx, sqlQuery, wr)
	if err != nil {
		return fmt.Errorf("failed to SQLReader.Read:%w", err)
	}

	if err := file.SaveAs(filePath); err != nil {
		return fmt.Errorf("failed to SaveAs:%w", err)
	}

	srv.logger.Infof("%d rows recorded", wr.rowsCount)
	srv.logger.Infof("%d rows skipped", wr.rowsSkip)

	if excelCfg.NeedPivot {
		if wr.columnsBeginData == 0 || wr.rowsCount == 0 {
			srv.logger.Infof("columnsBeginData=%d rowsCount=%d", wr.columnsBeginData, wr.rowsCount)
			return nil
		}

		srv.logger.Infof("adding pivot")
		if err := srv.createPivot(ctx, filePath, wr); err != nil {
			return fmt.Errorf("failed creatPivot:%w", err)
		}
	}

	return nil
}

func autofit(f *excelize.File, sheetName string) error {
	cols, err := f.GetCols(sheetName)
	if err != nil {
		return err
	}
	for idx, col := range cols {
		headerWidth := utf8.RuneCountInString(col[0]) + 2 // + 2 for margin
		name, err := excelize.ColumnNumberToName(idx + 1)
		if err != nil {
			return err
		}
		if err := f.SetColWidth(sheetName, name, name, float64(headerWidth)); err != nil {
			return err
		}
	}
	return nil
}

func coordinatesToCellName(col, row int, abs bool) string {
	coor, _ := excelize.CoordinatesToCellName(col, row, abs)
	return coor
}

//
type ExcelWriter struct {
	w                *excelize.StreamWriter
	rowsCount        int
	rowsSkip         int
	rowsLimit        int // 1_048_576
	columns          []string
	columnsBeginData int

	logger *zap.SugaredLogger
}

type ExcelWriterConfig struct {
	StreamWriter *excelize.StreamWriter
	Logger       *zap.SugaredLogger

	RowsLimit int
}

func NewExcelWriter(cfg ExcelWriterConfig) *ExcelWriter {
	ew := &ExcelWriter{
		w:         cfg.StreamWriter,
		logger:    cfg.Logger,
		rowsLimit: cfg.RowsLimit,
	}
	if ew.rowsLimit == 0 || ew.rowsLimit > EXCEL_LIMIT_ROWS {
		ew.rowsLimit = EXCEL_LIMIT_ROWS
	}

	return ew
}

func (wr *ExcelWriter) WriteStrings(data []string) error {
	wr.columns = data
	row := make([]interface{}, len(data))
	for i, v := range data {
		row[i] = v

		// begin of block of data
		// after [withComp]
		if wr.columnsBeginData == 0 && strings.Contains(v, "withComp") {
			wr.columnsBeginData = i + 1
		}
	}
	wr.rowsCount++
	cell, _ := excelize.CoordinatesToCellName(1, wr.rowsCount)
	return wr.w.SetRow(cell, row)
}

func (wr *ExcelWriter) Write(data []interface{}) error {
	// TODO
	if wr.rowsCount >= wr.rowsLimit {
		wr.rowsSkip++
		return nil
	}

	row := make([]interface{}, len(data))
	for i, v := range data {
		b := copyBytes(*(v.(*sql.RawBytes)))
		if len(b) == 0 {
			continue
		}

		if wr.columnsBeginData > i {
			row[i] = b
		} else {
			f, err := strconv.ParseFloat(string(b), 64)
			if err != nil {
				wr.logger.Errorf("failed to parse %s:%w", string(b), err)
				continue
			}
			row[i] = f
		}
	}
	wr.rowsCount++
	cell, _ := excelize.CoordinatesToCellName(1, wr.rowsCount)
	return wr.w.SetRow(cell, row)
}

func copyBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func (wr *ExcelWriter) Flush() error {
	return wr.w.Flush()
}
