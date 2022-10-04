package report

type ReportConfig struct {
	FilePath string
	SQLQuery string
	ExcelConfig

	Data interface{}
}

type ExcelConfig struct {
	NeedPivot bool
}
