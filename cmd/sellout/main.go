package main

import (
	"os"
	"os/signal"

	"github.com/germangorelkin/kam4-exporter/internal/db"
	"github.com/germangorelkin/kam4-exporter/internal/email"
	"github.com/germangorelkin/kam4-exporter/internal/rabbitmq"
	"github.com/germangorelkin/kam4-exporter/internal/service"

	"github.com/sirupsen/logrus"
)

func main() {
	connDB := os.Getenv("DATABASE_URL")
	if connDB == "" {
		logrus.Fatal("DATABASE_URL is not set.")
	}
	amqp := os.Getenv("AMQP")
	if amqp == "" {
		logrus.Fatal("AMQP is not set.")
	}
	storageHost := os.Getenv("IMAGE_STORAGE_HOST")
	if storageHost == "" {
		logrus.Fatal("IMAGE_STORAGE_HOST is not set.")
	}
	storagePath := os.Getenv("IMAGE_STORAGE_PATH")
	if storagePath == "" {
		logrus.Fatal("IMAGE_STORAGE_PATH is not set.")
	}
	emailLogin := os.Getenv("EMAIL_LOGIN")
	if emailLogin == "" {
		logrus.Fatal("EMAIL_LOGIN is not set.")
	}
	emailPassword := os.Getenv("EMAIL_PASSWORD")
	if emailPassword == "" {
		logrus.Fatal("EMAIL_PASSWORD is not set.")
	}
	emailHost := os.Getenv("EMAIL_HOST")
	if emailHost == "" {
		logrus.Fatal("EMAIL_HOST is not set.")
	}
	emailPort := os.Getenv("EMAIL_PORT")
	if emailPort == "" {
		logrus.Fatal("EMAIL_PORT is not set.")
	}

	Formatter := new(logrus.TextFormatter)
	Formatter.TimestampFormat = "02-01-2006 15:04:05"
	Formatter.FullTimestamp = true
	Formatter.DisableColors = true
	logrus.SetFormatter(Formatter)

	dbClient := db.NewRepository(connDB)
	mqClient := rabbitmq.New(rabbitmq.SessionConfig{
		ExchangeName: "topic_exporter",
		ExchangeType: "topic",
		QueueName:    "exporter",
		BindingKey:   "exporter",
		Addr:         amqp,
	})
	emailClient := email.NewSender(email.SenderConfig{
		From:     emailLogin,
		Password: emailPassword,
		Host:     emailHost,
		Port:     emailPort,
	})

	srv := service.NewSelloutService(service.SelloutServiceConfig{
		DB:       dbClient,
		MQ:       mqClient,
		Email:    emailClient,
		FilePath: storagePath,
		FileLink: storageHost,
	})
	srv.Run()

	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt)
	<-sigint

	_ = mqClient.Close()

	logrus.Println("sellout stopped.")
}
