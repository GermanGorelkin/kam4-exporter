package main

import (
	"os"
	"os/signal"

	"github.com/germangorelkin/kam4-exporter/internal/db"
	"github.com/germangorelkin/kam4-exporter/internal/email"
	"github.com/germangorelkin/kam4-exporter/internal/rabbitmq"
	"github.com/germangorelkin/kam4-exporter/internal/service"

	"go.uber.org/zap"
)

const (
	serviceName = "sellout-exporter"
	serviceVersion = "0.5.0"
)

type mainConfig struct {
	connDB        string
	amqp          string
	storageHost   string
	storagePath   string
	emailLogin    string
	emailPassword string
	emailHost     string
	emailPort     string
	logger        *zap.SugaredLogger
}

type fnClose func() error

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	appLogger := logger.Sugar().Named(serviceName + serviceVersion)
	appLogger.Info("The application is starting...")

	cfg := mainConfig{logger: appLogger}

	cfg.connDB = os.Getenv("DATABASE_URL")
	if cfg.connDB == "" {
		appLogger.Fatal("DATABASE_URL is not set.")
	}
	cfg.amqp = os.Getenv("AMQP")
	if cfg.amqp == "" {
		appLogger.Fatal("AMQP is not set.")
	}
	cfg.storageHost = os.Getenv("IMAGE_STORAGE_HOST")
	if cfg.storageHost == "" {
		appLogger.Fatal("IMAGE_STORAGE_HOST is not set.")
	}
	cfg.storagePath = os.Getenv("IMAGE_STORAGE_PATH")
	if cfg.storagePath == "" {
		appLogger.Fatal("IMAGE_STORAGE_PATH is not set.")
	}
	cfg.emailLogin = os.Getenv("EMAIL_LOGIN")
	if cfg.emailLogin == "" {
		appLogger.Fatal("EMAIL_LOGIN is not set.")
	}
	cfg.emailPassword = os.Getenv("EMAIL_PASSWORD")
	if cfg.emailPassword == "" {
		appLogger.Fatal("EMAIL_PASSWORD is not set.")
	}
	cfg.emailHost = os.Getenv("EMAIL_HOST")
	if cfg.emailHost == "" {
		appLogger.Fatal("EMAIL_HOST is not set.")
	}
	cfg.emailPort = os.Getenv("EMAIL_PORT")
	if cfg.emailPort == "" {
		appLogger.Fatal("EMAIL_PORT is not set.")
	}

	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt)
	<-sigint

	c, err := realMain(cfg)
	if err != nil {
		appLogger.Errorw("Got an error from realMain", "err", err)
	}
	if err = c(); err != nil {
		appLogger.Errorw("Got an error from fnClose", "err", err)
	}

	appLogger.Info("The application is stopped.")
}

func realMain(cfg mainConfig) (fnClose, error) {
	dbClient := db.NewRepository(cfg.connDB)
	mqClient := rabbitmq.New(rabbitmq.SessionConfig{
		ExchangeName: "topic_exporter",
		ExchangeType: "topic",
		QueueName:    "exporter",
		BindingKey:   "exporter",
		Addr:         cfg.amqp,
	})
	emailClient := email.NewSender(email.SenderConfig{
		From:     cfg.emailLogin,
		Password: cfg.emailPassword,
		Host:     cfg.emailHost,
		Port:     cfg.emailPort,
	})

	srv := service.NewSelloutService(service.SelloutServiceConfig{
		DB:       dbClient,
		MQ:       mqClient,
		Email:    emailClient,
		FilePath: cfg.storagePath,
		FileLink: cfg.storageHost,
		Logger:   cfg.logger.Named("sellout-service"),
	})
	srv.Run()

	return func() error { return mqClient.Close() }, nil
}