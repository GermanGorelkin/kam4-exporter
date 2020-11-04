package email

import (
	"fmt"
	"net/smtp"
)

var templateSelloutExport =
	      "From: %s\n" +
	      "To: %s\n" +
		  "Subject: Sellout export\n\n" +
	      "%s"

type Sender struct {
	from     string
	password string
	server   smtpServer
	auth     smtp.Auth
}

type SenderConfig struct {
	From     string
	Password string
	Host     string
	Port     string
}

type smtpServer struct {
	host string
	port string
}
func (s *smtpServer) address() string {
	return s.host + ":" + s.port
}

func NewSender(cfg SenderConfig) Sender {
	return Sender{
		from:     cfg.From,
		password: cfg.Password,
		server: smtpServer{
			host: cfg.Host,
			port: cfg.Port,
		},
		auth: smtp.PlainAuth("", cfg.From, cfg.Password, cfg.Host),
	}
}

func (s Sender) Send(receivers []string, msg string) error {
	body := fmt.Sprintf(fmt.Sprintf(templateSelloutExport, s.from, receivers[0], msg))
	return smtp.SendMail(s.server.address(), s.auth, s.from, receivers, []byte(body))
}