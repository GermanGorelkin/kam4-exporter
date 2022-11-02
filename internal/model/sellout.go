package model

type SelloutOptions struct {
	Period          string `db:"period"`
	DataSplit       string `db:"data_split"`
	DetailsType     string `db:"details_type"`
	Clients         string `db:"clients"`
	DataFrom        string `db:"data_from"`
	WithCompetitors string `db:"with_competitors"`
	Category        string `db:"category"`
	Subcategory     string `db:"subcategory"`
	Manufacturer    string `db:"manufacturer"`
	Brand           string `db:"brand"`
	ValueType       string `db:"value_type"`
	WithVat         string `db:"with_vat"`
	Wholesale       string `db:"wholesale"`
	UserEmail       string `db:"user_email"`
	FirstClient     string `db:"first_client"`

	NeedSendEmail bool `db:"need_send_email"`
}
