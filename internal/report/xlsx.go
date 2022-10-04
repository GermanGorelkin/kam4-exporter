package report

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/germangorelkin/kam4-exporter/internal/model"
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
	return "xlsx"
}

func (srv XLSXReport) Build(ctx context.Context, cfg ReportConfig) error {
	opts := cfg.Data.(model.SelloutOptions)
	return srv.exportData(ctx, cfg.FilePath, cfg.SQLQuery, cfg.ExcelConfig, opts)
}

/*
1. Option name	Description
2. Period	01.01.2021 - 01.06.2022
3. Data split	Month
4. Details type	Network
5. Clients	Tander (Magnit); Perekrestok
6. Data from	Client
7. With competitors	Yes
8. Category	All
9. Subcategory	All
10. Manufacturer	All
11. Brand	All
12. Value type	All
13. With vat	Yes
14. Wholesale	All
*/
func (srv XLSXReport) createOptions(ctx context.Context, filePath string, opts model.SelloutOptions) error {
	file, err := excelize.OpenFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to OpenFile %s:%w", filePath, err)
	}
	_ = file.NewSheet("options")

	// header
	if err := srv.addHeaderForOptions(file); err != nil {
		return fmt.Errorf("failed to addHeaderForOptions:%w", err)
	}

	// values
	if err := file.SetCellValue("options", "A2", "Period"); err != nil {
		return fmt.Errorf("failed to SetCellValue(A2):%w", err)
	}
	if err := file.SetCellValue("options", "B2", opts.Period); err != nil {
		return fmt.Errorf("failed to SetCellValue(B2):%w", err)
	}
	if err := file.SetCellValue("options", "A3", "Data split"); err != nil {
		return fmt.Errorf("failed to SetCellValue(A3):%w", err)
	}
	if err := file.SetCellValue("options", "B3", opts.DataSplit); err != nil {
		return fmt.Errorf("failed to SetCellValue(B3):%w", err)
	}
	if err := file.SetCellValue("options", "A4", "Details type"); err != nil {
		return fmt.Errorf("failed to SetCellValue(A4):%w", err)
	}
	if err := file.SetCellValue("options", "B4", opts.DetailsType); err != nil {
		return fmt.Errorf("failed to SetCellValue(B4):%w", err)
	}
	if err := file.SetCellValue("options", "A5", "Clients"); err != nil {
		return fmt.Errorf("failed to SetCellValue(A5):%w", err)
	}
	if err := file.SetCellValue("options", "B5", opts.Clients); err != nil {
		return fmt.Errorf("failed to SetCellValue(B5):%w", err)
	}
	if err := file.SetCellValue("options", "A6", "Data from"); err != nil {
		return fmt.Errorf("failed to SetCellValue(A6):%w", err)
	}
	if err := file.SetCellValue("options", "B6", opts.DataFrom); err != nil {
		return fmt.Errorf("failed to SetCellValue(B6):%w", err)
	}
	if err := file.SetCellValue("options", "A7", "With competitors"); err != nil {
		return fmt.Errorf("failed to SetCellValue(A7):%w", err)
	}
	if err := file.SetCellValue("options", "B7", opts.WithCompetitors); err != nil {
		return fmt.Errorf("failed to SetCellValue(B7):%w", err)
	}
	if err := file.SetCellValue("options", "A8", "Category"); err != nil {
		return fmt.Errorf("failed to SetCellValue(A8):%w", err)
	}
	if err := file.SetCellValue("options", "B8", opts.Category); err != nil {
		return fmt.Errorf("failed to SetCellValue(B8):%w", err)
	}
	if err := file.SetCellValue("options", "A9", "Subcategory"); err != nil {
		return fmt.Errorf("failed to SetCellValue(A9):%w", err)
	}
	if err := file.SetCellValue("options", "B9", opts.Subcategory); err != nil {
		return fmt.Errorf("failed to SetCellValue(B9):%w", err)
	}
	if err := file.SetCellValue("options", "A10", "Manufacturer"); err != nil {
		return fmt.Errorf("failed to SetCellValue(A10):%w", err)
	}
	if err := file.SetCellValue("options", "B10", opts.Manufacturer); err != nil {
		return fmt.Errorf("failed to SetCellValue(B10):%w", err)
	}
	if err := file.SetCellValue("options", "A11", "Brand"); err != nil {
		return fmt.Errorf("failed to SetCellValue(A11):%w", err)
	}
	if err := file.SetCellValue("options", "B11", opts.Brand); err != nil {
		return fmt.Errorf("failed to SetCellValue(B11):%w", err)
	}
	if err := file.SetCellValue("options", "A12", "Value type"); err != nil {
		return fmt.Errorf("failed to SetCellValue(A12):%w", err)
	}
	if err := file.SetCellValue("options", "B12", opts.ValueType); err != nil {
		return fmt.Errorf("failed to SetCellValue(B12):%w", err)
	}
	if err := file.SetCellValue("options", "A13", "With vat"); err != nil {
		return fmt.Errorf("failed to SetCellValue(A13):%w", err)
	}
	if err := file.SetCellValue("options", "B13", opts.WithVat); err != nil {
		return fmt.Errorf("failed to SetCellValue(B13):%w", err)
	}
	if err := file.SetCellValue("options", "A14", "Wholesale"); err != nil {
		return fmt.Errorf("failed to SetCellValue(A14):%w", err)
	}
	if err := file.SetCellValue("options", "B14", opts.Wholesale); err != nil {
		return fmt.Errorf("failed to SetCellValue(B14):%w", err)
	}

	// file save
	if err := file.SaveAs(filePath); err != nil {
		return fmt.Errorf("failed to SaveAs:%w", err)
	}

	return nil
}

func (srv XLSXReport) addHeaderForOptions(file *excelize.File) error {
	style, err := file.NewStyle(&excelize.Style{
		Font: &excelize.Font{
			Bold: true,
			// Italic: true,
			// Family: "Times New Roman",
			// Size:   36,
			// Color:  "#777777",

		},
	})
	if err != nil {
		return fmt.Errorf("failed to NewStyle:%w", err)
	}

	if err := file.SetCellValue("options", "A1", "Option name"); err != nil {
		return fmt.Errorf("failed to SetCellValue(A1):%w", err)
	}
	if err := file.SetCellValue("options", "B1", "Description"); err != nil {
		return fmt.Errorf("failed to SetCellValue(B1):%w", err)
	}

	if err := file.SetCellStyle("options", "A1", "B1", style); err != nil {
		return fmt.Errorf("failed to SetCellStyle:%w", err)
	}

	return nil
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
		Rows:              []excelize.PivotTableField{{Data: "Клиент"}},
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

// func (srv XLSXReport) addStylesForPivot(ctx context.Context, filePath string) error {
// 	file, err := excelize.OpenFile(filePath)
// 	if err != nil {
// 		return fmt.Errorf("failed to OpenFile %s:%w", filePath, err)
// 	}

// 	// style
// 	style, _ := file.NewStyle(`{"number_format": 4}`)
// 	if err := file.SetCellStyle("pivot", "B5", "B5", style); err != nil {
// 		return fmt.Errorf("failed to SetColStyle:%w", err)
// 	}

// 	if err := file.SaveAs(filePath); err != nil {
// 		return fmt.Errorf("failed to SaveAs:%w", err)
// 	}

// 	return nil
// }

func (srv XLSXReport) exportData(ctx context.Context, filePath string, sqlQuery string, excelCfg ExcelConfig, opts model.SelloutOptions) error {
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
		// if err := srv.addStylesForPivot(ctx, filePath); err != nil {
		// 	return fmt.Errorf("failed addStylesForPivot:%w", err)
		// }
	}

	srv.logger.Infof("adding options")
	if err := srv.createOptions(ctx, filePath, opts); err != nil {
		return fmt.Errorf("failed createOptions:%w", err)
	}

	return nil
}

// func autofit(f *excelize.File, sheetName string) error {
// 	cols, err := f.GetCols(sheetName)
// 	if err != nil {
// 		return err
// 	}
// 	for idx, col := range cols {
// 		headerWidth := utf8.RuneCountInString(col[0]) + 2 // + 2 for margin
// 		name, err := excelize.ColumnNumberToName(idx + 1)
// 		if err != nil {
// 			return err
// 		}
// 		if err := f.SetColWidth(sheetName, name, name, float64(headerWidth)); err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

func coordinatesToCellName(col, row int, abs bool) string {
	coor, _ := excelize.CoordinatesToCellName(col, row, abs)
	return coor
}

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
