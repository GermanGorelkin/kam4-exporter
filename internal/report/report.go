package report

type ReportConfig struct {
	FilePath string
	SQLQuery string
	ExcelConfig
}

type ExcelConfig struct {
	NeedPivot bool
}
