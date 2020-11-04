package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/germangorelkin/sql2csv"
)

type PubSub interface {
	Subscribe(func(b []byte) error)
}
type EmailSender interface {
	Send(receivers []string, msg string) error
}
type Repository interface {
	GetDB() *sql.DB
	GetUserEmail(userID int) ([]string, error)
}

type SelloutService struct {
	DB        Repository
	MQ        PubSub
	Email     EmailSender
	FileStore fileStore
}

type fileStore struct {
	path string
	link string
}

type SelloutServiceConfig struct {
	DB       Repository
	MQ       PubSub
	Email    EmailSender
	FilePath string
	FileLink string
}

func NewSelloutService(cfg SelloutServiceConfig) SelloutService {
	return SelloutService{
		DB:    cfg.DB,
		MQ:    cfg.MQ,
		Email: cfg.Email,
		FileStore: fileStore{
			path: cfg.FilePath,
			link: cfg.FileLink,
		},
	}
}

type SelloutRequest struct {
	UserId int         `json:"user_id"`
	Param  interface{} `json:"param"`
}

func (srv SelloutService) Run() {
	srv.MQ.Subscribe(func(b []byte) error {
		err := srv.handleSellout(b)
		if err != nil {
			logrus.Error(err)
		}
		return nil
	})
}

func (srv SelloutService) handleSellout(b []byte) error {
	var err error

	var req SelloutRequest
	if err = json.Unmarshal(b, &req); err != nil {
		return fmt.Errorf("error unmarshal:%s", err)
	}

	fileName := srv.genUniqueFileName()
	if err = srv.exportData(req, fileName); err != nil {
		return err
	}

	var email []string
	if email, err = srv.DB.GetUserEmail(req.UserId); err != nil {
		return err
	}

	flink := fmt.Sprintf("%s/%s", srv.FileStore.link, fileName)
	if err = srv.Email.Send(email, flink); err != nil {
		return err
	}

	return nil
}

func (srv SelloutService) genUniqueFileName() string {
	return fmt.Sprintf("%d.csv", time.Now().UnixNano())
}

func (srv SelloutService) exportData(req SelloutRequest, fileName string) error {
	rd := sql2csv.SQLReader{DB: srv.DB.GetDB()}
	rd.Columns = true

	fpath := filepath.Join(srv.FileStore.path, fileName)
	fd, err := os.Create(fpath)
	if err != nil {
		return err
	}
	defer fd.Close()

	if err = addBOM(fd); err != nil {
		return err
	}

	csvWriter := sql2csv.NewCSVWriter([]byte(";"), []byte("\r\n"), fd)

	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	param, err := json.Marshal(req.Param)
	if err != nil {
		return err
	}

	query := fmt.Sprintf("exec [api].[Sellout_Export] @userID=%d, @data=N'%s';", req.UserId, string(param))
	logrus.Println(query)
	err = rd.Read(ctx, query, csvWriter)
	if err != nil {
		return err
	}

	return nil
}

func addBOM(w io.Writer) error {
	if _, err := w.Write([]byte{0xEF, 0xBB, 0xBF}); err != nil {
		return err
	}
	return nil
}