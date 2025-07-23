package metric

type Service interface {
	DurationSelloutExport(val float64, code, source string)
	TotalSelloutExport(code, source string)
}
