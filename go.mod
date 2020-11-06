module github.com/germangorelkin/kam4-exporter

go 1.15

replace github.com/germangorelkin/sql2csv => ../sql2csv

require (
	github.com/Rican7/retry v0.1.0
	github.com/denisenkom/go-mssqldb v0.0.0-20191128021309-1d7a30a10f73
	github.com/germangorelkin/sql2csv v0.5.0
	github.com/sirupsen/logrus v1.7.0
	github.com/streadway/amqp v1.0.0
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.16.0 // indirect
)
