package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/germangorelkin/kam4-exporter/internal/metric"
	"github.com/germangorelkin/kam4-exporter/internal/report"
	"go.uber.org/zap"
)

type PubSub interface {
	Subscribe(func(b []byte) error)
}
type EmailSender interface {
	Send(receivers []string, subject, msg string) error
}
type Repository interface {
	GetUserEmail(userID int) ([]string, error)
	GetClientName(id int) (string, error)
}
type Report interface {
	Build(ctx context.Context, cfg report.ReportConfig) error
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
	UserID int `json:"user_id"`
	Param  struct {
		BeginDate string `json:"begin_date"`
		EndDate   string `json:"end_date"`
		Period    string `json:"period"`
		Details   string `json:"details"`
		Clients   []struct {
			ID      int   `json:"id"`
			Formats []int `json:"formats,omitempty"`
		} `json:"clients"`
		DataFrom string `json:"data_from"`
		Products []struct {
			Manufacturerid int `json:"manufacturerID,omitempty"`
			Categoryid     int `json:"categoryID,omitempty"`
			Subcategoryid  int `json:"subcategoryID,omitempty"`
			Brandid        int `json:"brandID,omitempty"`
		} `json:"products"`
		ValueType       []string `json:"value_type"`
		WithCompetitors int      `json:"with_competitors"`
		Wholesale       string   `json:"wholesale"`
		WithVAT         int      `json:"with_vat,omitempty"`
	} `json:"param"`
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

	if len(req.Param.Clients) == 0 {
		return fmt.Errorf("error: clients are not set")
	}

	email, err := srv.DB.GetUserEmail(req.UserID)
	if err != nil {
		return fmt.Errorf("failed to GetUserEmail(%d): %w", req.UserID, err)
	}
	srv.logger.Info("GetUserEmail completed successfully")
	srv.logger.Debugf("%v for userID=%d", email, req.UserID)

	fileName := srv.genUniqueFileName(srv.Report.FileExtension())
	if err := srv.exportData(ctx, req, fileName); err != nil {
		return fmt.Errorf("failed to exportData(%s): %w", fileName, err)
	}
	srv.logger.Info("ExportData completed successfully")

	clientName, err := srv.DB.GetClientName(req.Param.Clients[0].ID)
	if err != nil {
		return fmt.Errorf("failed to GetClientName(%d): %w", req.Param.Clients[0].ID, err)
	}
	subject := buildSubject(clientName, req)
	flink := fmt.Sprintf("%s/%s", srv.FileStore.link, fileName)
	if err := srv.Email.Send(email, subject, flink); err != nil {
		return fmt.Errorf("failed to EmailSend(%s,%s): %w", email, flink, err)
	}
	srv.logger.Info("EmailSend completed successfully")

	return nil
}

func (srv SelloutService) genUniqueFileName(extension string) string {
	return fmt.Sprintf("%d.%s", time.Now().UnixNano(), extension)
}

func (srv SelloutService) exportData(ctx context.Context, req SelloutRequest, fileName string) error {
	query, err := buildSQLQuery(req)
	if err != nil {
		return fmt.Errorf("failed to build sql query:%w", err)
	}

	cfg := report.ReportConfig{
		FilePath:    filepath.Join(srv.FileStore.path, fileName),
		SQLQuery:    query,
		ExcelConfig: buildExcelConfig(req),
	}
	return srv.Report.Build(ctx, cfg)
}

func buildExcelConfig(req SelloutRequest) report.ExcelConfig {
	needPivot := false
	if req.Param.Period == "month" && req.Param.Details == "network" {
		needPivot = true
	}

	return report.ExcelConfig{
		NeedPivot: needPivot,
	}
}

func buildSQLQuery(req SelloutRequest) (string, error) {
	param, err := json.Marshal(req.Param)
	if err != nil {
		return "", fmt.Errorf("failed to marshal %#v: %w", req.Param, err)
	}
	return fmt.Sprintf("exec [api].[Sellout_Export] @userID=%d, @data=N'%s';", req.UserID, string(param)), nil
}

//"Sellout export_{Название клиента}{(C) - если данные с конкурентами. Если данные без конкурентов, то пусто}{TypeDate}_{Период}"
//
func buildSubject(clientName string, req SelloutRequest) string {
	var buf bytes.Buffer

	buf.WriteString("Sellout export_")
	buf.WriteString(clientName)
	if req.Param.WithCompetitors == 1 {
		buf.WriteString("(C)")
	}
	buf.WriteString("_" + req.Param.Period + "_")
	buf.WriteString(req.Param.BeginDate + "_")
	buf.WriteString(req.Param.EndDate)

	return buf.String()
}
