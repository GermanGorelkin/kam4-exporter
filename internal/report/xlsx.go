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

func (srv XLSXReport) Build(ctx context.Context, filePath string, sqlQuery string) error {
	return srv.exportData(ctx, filePath, sqlQuery)
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
	dataRange := fmt.Sprintf("data!%s:%s", coordinatesToCellName(1, 1, true), coordinatesToCellName(len(wr.cols), wr.rowNum, true))

	pivotDatas := make([]excelize.PivotTableField, 0, 1)
	for i := wr.colNumData; i < len(wr.cols); i++ {
		pivotDatas = append(pivotDatas, excelize.PivotTableField{
			Data:     wr.cols[i],
			Name:     wr.cols[i],
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

func (srv XLSXReport) exportData(ctx context.Context, filePath string, sqlQuery string) error {
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

	srv.logger.Infof("writed %d rows", wr.rowNum)

	if wr.colNumData == 0 || wr.rowNum == 0 {
		return nil
	}

	if err := srv.createPivot(ctx, filePath, wr); err != nil {
		return fmt.Errorf("failed creatPivot:%w", err)
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
	w          *excelize.StreamWriter
	rowNum     int
	cols       []string
	colNumData int

	logger *zap.SugaredLogger
}

type ExcelWriterConfig struct {
	StreamWriter *excelize.StreamWriter
	Logger       *zap.SugaredLogger
}

func NewExcelWriter(cfg ExcelWriterConfig) *ExcelWriter {
	return &ExcelWriter{w: cfg.StreamWriter, logger: cfg.Logger}
}

func (wr *ExcelWriter) WriteStrings(data []string) error {
	wr.cols = data
	row := make([]interface{}, len(data))
	for i, v := range data {
		row[i] = v

		// begin of block of data
		// after [withComp]
		if wr.colNumData == 0 && strings.Contains(v, "withComp") {
			wr.colNumData = i + 1
		}
	}
	wr.rowNum++
	cell, _ := excelize.CoordinatesToCellName(1, wr.rowNum)
	return wr.w.SetRow(cell, row)
}

func (wr *ExcelWriter) Write(data []interface{}) error {
	row := make([]interface{}, len(data))
	for i, v := range data {
		b := copyBytes(*(v.(*sql.RawBytes)))
		if len(b) == 0 {
			continue
		}

		if wr.colNumData > i {
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
	wr.rowNum++
	cell, _ := excelize.CoordinatesToCellName(1, wr.rowNum)
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
