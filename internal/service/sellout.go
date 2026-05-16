package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/germangorelkin/kam4-exporter/internal/metric"
	"github.com/germangorelkin/kam4-exporter/internal/model"
	"github.com/germangorelkin/kam4-exporter/internal/report"
	"github.com/germangorelkin/kam4-exporter/internal/storage"
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
	GetSelloutOptions(data string) (model.SelloutOptions, error)
}
type Report interface {
	Build(ctx context.Context, cfg report.ReportConfig) error
	FileExtension() string
}

type SelloutService struct {
	DB        Repository
	MQ        PubSub
	Email     EmailSender
	FileStore storage.Storage
	Report    Report

	metrics metric.Service
	logger  *zap.SugaredLogger
}

type SelloutServiceConfig struct {
	DB      Repository
	MQ      PubSub
	Report  Report
	Email   EmailSender
	Storage storage.Storage
	Logger  *zap.SugaredLogger
	Metrics metric.Service
}

func NewSelloutService(cfg SelloutServiceConfig) SelloutService {
	return SelloutService{
		DB:        cfg.DB,
		MQ:        cfg.MQ,
		Email:     cfg.Email,
		Report:    cfg.Report,
		FileStore: cfg.Storage,
		logger:    cfg.Logger,
		metrics:   cfg.Metrics,
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
			ID              int    `json:"id"`
			Formats         []int  `json:"formats,omitempty"`
			DataFrom        string `json:"data_from,omitempty"`
			WithCompetitors int    `json:"with_competitors"`
		} `json:"clients"`
		DataFrom string `json:"data_from"`
		Products []struct {
			Manufacturerid string `json:"manufacturerID,omitempty"`
			Categoryid     string `json:"categoryID,omitempty"`
			Subcategoryid  string `json:"subcategoryID,omitempty"`
			Brandid        string `json:"brandID,omitempty"`
		} `json:"products"`
		ValueType       []string `json:"value_type"`
		WithCompetitors int      `json:"with_competitors"`
		Wholesale       string   `json:"wholesale"`
		WithVAT         int      `json:"with_vat,omitempty"`
		Language        string   `json:"language,omitempty"`
		MoneyType       string   `json:"money_type,omitempty"`
		Regions         []int    `json:"regions,omitempty"`
	} `json:"param"`

	DataRaw []byte
}

func (s SelloutRequest) Validator() error {
	if len(s.Param.Clients) == 0 {
		return fmt.Errorf("clients must be set")
	}

	return nil
}

func (srv SelloutService) Run(ctx context.Context) {
	srv.MQ.Subscribe(func(b []byte) error {
		code := "success"
		started := time.Now()
		defer func() {
			srv.metrics.DurationSelloutExport(time.Since(started).Seconds(), code, "mq")
			srv.metrics.TotalSelloutExport(code, "mq")
		}()

		srv.logger.Infow("Received request from MQ", "data", string(b))
		_, err := srv.handleSellout(ctx, b) // handle
		if err != nil {
			code = "fail"
			srv.logger.Errorw("Got an error from handleSellout", "error", err)
		}
		srv.logger.Infow("Request from MQ processing completed", "data", string(b))
		return nil
	})
}

func (srv SelloutService) HandleSelloutExport(ctx context.Context, b []byte) (string, error) {
	code := "success"
	started := time.Now()
	defer func() {
		srv.metrics.DurationSelloutExport(time.Since(started).Seconds(), code, "http")
		srv.metrics.TotalSelloutExport(code, "http")
	}()

	srv.logger.Infow("Received request from HTTP Server", "data", string(b))
	link, err := srv.handleSellout(ctx, b)
	if err != nil {
		srv.logger.Errorw("Got an error from handleSellout", "error", err)
		code = "fail"
		return link, err
	}
	srv.logger.Infow("Request from HTTP Server processing completed", "data", string(b))

	return link, nil
}

func (srv SelloutService) handleSellout(ctx context.Context, b []byte) (string, error) {
	var flink string

	var req SelloutRequest
	if err := json.Unmarshal(b, &req); err != nil {
		return flink, fmt.Errorf("failed to unmarshal %s: %w", string(b), err)
	}
	// req.DataRaw = b
	if err := req.Validator(); err != nil {
		return flink, err
	}

	// get sellout options
	opts, err := srv.DB.GetSelloutOptions(string(b))
	if err != nil {
		return flink, fmt.Errorf("failed to GetSelloutOptions %s: %w", string(b), err)
	}

	// gen fileName
	fileName := storage.UniqueFileName(srv.Report.FileExtension())

	// export
	srv.logger.Infow("Starting export of data to file", "file", fileName)
	if err := srv.exportData(ctx, req, fileName, opts); err != nil {
		srv.logger.Errorw("Failed to export data to file", "file", fileName, "error", err)
		return flink, fmt.Errorf("failed to exportData(%s): %w", fileName, err)
	}
	srv.logger.Infow("Data export to file completed successfully", "file", fileName)

	// gen link
	flink, err = srv.FileStore.GetFileLink(fileName)
	if err != nil {
		return flink, fmt.Errorf("failed to GetFileLink(%s): %w", fileName, err)
	}

	// send email
	if opts.NeedSendEmail {
		subject := buildSubject(opts.FirstClient, req)
		if err := srv.Email.Send([]string{opts.UserEmail}, subject, flink); err != nil {
			return flink, fmt.Errorf("failed to EmailSend(%s,%s): %w", opts.UserEmail, flink, err)
		}
		srv.logger.Infow("Email sent successfully", "to", opts.UserEmail, "subject", subject, "link", flink)
	}

	return flink, nil
}

func (srv SelloutService) exportData(ctx context.Context, req SelloutRequest, fileName string, opts model.SelloutOptions) error {
	query, err := buildSQLQuery(req)
	if err != nil {
		return fmt.Errorf("failed to build sql query:%w", err)
	}
	srv.logger.Infow("SQL query generated for export", "query", query)

	cfg := report.ReportConfig{
		FilePath:    srv.FileStore.GetFilePath(fileName),
		SQLQuery:    query,
		ExcelConfig: buildExcelConfig(req),
		Data:        opts,
	}
	return srv.Report.Build(ctx, cfg)
}

func buildExcelConfig(req SelloutRequest) report.ExcelConfig {
	needPivot := false
	if req.Param.Period == "month" && req.Param.Details == "client" {
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

// "Sellout export_{Название клиента}{(C) - если данные с конкурентами. Если данные без конкурентов, то пусто}{TypeDate}_{Период}"
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
