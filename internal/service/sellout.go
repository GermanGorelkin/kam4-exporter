package service

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/germangorelkin/kam4-exporter/internal/metric"
	"go.uber.org/zap"
)

type PubSub interface {
	Subscribe(func(b []byte) error)
}
type EmailSender interface {
	Send(receivers []string, msg string) error
}
type Repository interface {
	GetUserEmail(userID int) ([]string, error)
}
type Report interface {
	Build(ctx context.Context, fileName string, sqlQuery string) error
	FileExtension() string
}

type SelloutService struct {
	DB        Repository
	MQ        PubSub
	Email     EmailSender
	FileStore fileStore
	Report    Report

	metrics metric.Service
	logger  *zap.SugaredLogger
}

type fileStore struct {
	path string
	link string
}

type SelloutServiceConfig struct {
	DB       Repository
	MQ       PubSub
	Report   Report
	Email    EmailSender
	FilePath string
	FileLink string
	Logger   *zap.SugaredLogger
	Metrics  metric.Service
}

func NewSelloutService(cfg SelloutServiceConfig) SelloutService {
	return SelloutService{
		DB:     cfg.DB,
		MQ:     cfg.MQ,
		Email:  cfg.Email,
		Report: cfg.Report,
		FileStore: fileStore{
			path: cfg.FilePath,
			link: cfg.FileLink,
		},
		logger:  cfg.Logger,
		metrics: cfg.Metrics,
	}
}

type SelloutRequest struct {
	UserId int         `json:"user_id"`
	Param  interface{} `json:"param"`
}

func (srv SelloutService) Run(ctx context.Context) {
	srv.MQ.Subscribe(func(b []byte) error {
		code := "success"
		started := time.Now()
		defer func() {
			srv.metrics.DurationSelloutExport(code, time.Since(started).Seconds())
			srv.metrics.TotalSelloutExport(code)
		}()

		srv.logger.Infof("Received request from MQ: %s", string(b))
		err := srv.handleSellout(ctx, b) // handle
		if err != nil {
			code = "fail"
			srv.logger.Errorw("Got an error from handleSellout", "err", err)
		}
		srv.logger.Infof("Request processing completed: %s", string(b))
		return nil
	})
}

func (srv SelloutService) handleSellout(ctx context.Context, b []byte) error {
	var req SelloutRequest
	if err := json.Unmarshal(b, &req); err != nil {
		return fmt.Errorf("failed to unmarshal %s: %w", string(b), err)
	}

	email, err := srv.DB.GetUserEmail(req.UserId)
	if err != nil {
		return fmt.Errorf("failed to GetUserEmail(%s): %w", email, err)
	}
	srv.logger.Info("GetUserEmail completed successfully")

	fileName := srv.genUniqueFileName(srv.Report.FileExtension())
	if err := srv.exportData(ctx, req, fileName); err != nil {
		return fmt.Errorf("failed to exportData(%s): %w", fileName, err)
	}
	srv.logger.Info("ExportData completed successfully")

	flink := fmt.Sprintf("%s/%s", srv.FileStore.link, fileName)
	if err := srv.Email.Send(email, flink); err != nil {
		return fmt.Errorf("failed to EmailSend(%s,%s): %w", email, flink, err)
	}
	srv.logger.Info("EmailSend completed successfully")

	return nil
}

func (srv SelloutService) genUniqueFileName(extension string) string {
	return fmt.Sprintf("%d.%s", time.Now().UnixNano(), extension)
}

func (srv SelloutService) exportData(ctx context.Context, req SelloutRequest, fileName string) (err error) {
	param, err := json.Marshal(req.Param)
	if err != nil {
		return fmt.Errorf("failed to marshal %s: %w", req.Param, err)
	}
	query := fmt.Sprintf("exec [api].[Sellout_Export] @userID=%d, @data=N'%s';", req.UserId, string(param))

	fpath := filepath.Join(srv.FileStore.path, fileName)

	return srv.Report.Build(ctx, fpath, query)
}
