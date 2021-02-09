package metric

type Service interface {
	DurationSelloutExport(code string, val float64)
	TotalSelloutExport(code string)
}
