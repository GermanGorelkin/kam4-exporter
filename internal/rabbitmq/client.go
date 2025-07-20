package rabbitmq

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

const (
	// When reconnecting to the server after connection failure
	reconnectDelay = 5 * time.Second

	// When setting up the channel after a channel exception
	reInitDelay = 2 * time.Second

	// When resending messages the server didn't confirm
	resendDelay = 5 * time.Second
)

var (
	ErrNotConnected  = errors.New("not connected to a server")
	ErrAlreadyClosed = errors.New("already closed: not connected to the server")
	ErrShutdown      = errors.New("session is shutting down")
)

type MsgSeen struct {
	seen map[string]bool
	mu   sync.RWMutex
}

func (ms *MsgSeen) Get(id string) bool {
	ms.mu.RLock()
	defer ms.mu.Unlock()
	return ms.seen[id]
}

func (ms *MsgSeen) Set(id string) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.seen[id] = true
}

func NewMsgSeen() *MsgSeen {
	return &MsgSeen{
		seen: map[string]bool{},
	}
}

type Session struct {
	exchangeName string
	exchangeType string
	queueName    string
	bindingKey   string
	consumerTag  string

	connection      *amqp.Connection
	channel         *amqp.Channel
	done            chan bool
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	isReady         bool
	close           chan struct{}

	logger  *zap.SugaredLogger
	msgSeen *MsgSeen
}

type SessionConfig struct {
	ExchangeName string
	ExchangeType string
	QueueName    string
	BindingKey   string
	Addr         string
	Logger       *zap.SugaredLogger
}

// New creates a new Session instance, and automatically
// attempts to connect to the server.
func New(cfg SessionConfig) *Session {
	session := Session{
		exchangeName: cfg.ExchangeName,
		exchangeType: cfg.ExchangeType,
		queueName:    cfg.QueueName,
		bindingKey:   cfg.BindingKey,

		//consumerTag: time.Now().String(),

		isReady: false,
		done:    make(chan bool),
		close:   make(chan struct{}),

		logger:  cfg.Logger,
		msgSeen: NewMsgSeen(),
	}
	go session.handleReconnect(cfg.Addr)
	return &session
}

func (session *Session) NotifyClose() <-chan struct{} {
	return session.close
}

func (session *Session) setState(s bool) {
	if s {
		session.isReady = true
		session.close = make(chan struct{})
	} else {
		session.isReady = false
		if session.close != nil {
			close(session.close)
			session.close = nil
		}
	}
}

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (session *Session) handleReconnect(addr string) {
	for {
		session.setState(false)
		session.logger.Info("Attempting to connect")

		conn, err := session.connect(addr)

		if err != nil {
			session.logger.Errorf("failed to connect: %s", err)
			session.logger.Info("Retrying...")

			select {
			case <-session.done:
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}

		if done := session.handleReInit(conn); done {
			break
		}
	}
}

// connect will create a new AMQP connection
func (session *Session) connect(addr string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(addr)
	if err != nil {
		err = fmt.Errorf("failed to dial to %s: %w", addr, err)
		return nil, err
	}

	session.changeConnection(conn)
	session.logger.Info("Connected!")
	return conn, nil
}

// handleReconnect will wait for a channel error
// and then continuously attempt to re-initialize both channels
func (session *Session) handleReInit(conn *amqp.Connection) bool {
	for {
		session.setState(false)

		err := session.init(conn)

		if err != nil {
			session.logger.Errorf("failed to initialize channel: %s", err)
			session.logger.Info("Retrying...")

			select {
			case <-session.done:
				return true
			case <-time.After(reInitDelay):
			}
			continue
		}

		select {
		case <-session.done:
			return true
		case <-session.notifyConnClose:
			session.logger.Info("Connection closed. Reconnecting...")
			return false
		case <-session.notifyChanClose:
			session.logger.Info("Channel closed. Re-running init...")
		}
	}
}

// init will initialize channel & declare queue
func (session *Session) init(conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to conn Channel: %w", err)
	}

	err = ch.Confirm(false)
	if err != nil {
		return fmt.Errorf("failed to Confirm: %w", err)
	}

	err = ch.ExchangeDeclare(
		session.exchangeName, // name
		session.exchangeType, // type
		false,                // durable
		false,                // auto-deleted
		false,                // internal
		false,                // no-wait
		nil,                  // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to ExchangeDeclare: %w", err)
	}

	_, err = ch.QueueDeclare(
		session.queueName,
		true,  // Durable
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		return fmt.Errorf("failed to QueueDeclare: %w", err)
	}

	err = ch.QueueBind(
		session.queueName,
		session.bindingKey,
		session.exchangeName,
		false,
		nil)
	if err != nil {
		return fmt.Errorf("failed to QueueBind: %w", err)
	}

	err = ch.Qos(1, 0, false)
	if err != nil {
		return fmt.Errorf("failed to Qos: %w", err)
	}

	session.changeChannel(ch)
	session.setState(true)
	session.logger.Info("Setup!")

	return nil
}

// changeConnection takes a new connection to the queue,
// and updates the close listener to reflect this.
func (session *Session) changeConnection(connection *amqp.Connection) {
	session.connection = connection
	session.notifyConnClose = make(chan *amqp.Error)
	session.connection.NotifyClose(session.notifyConnClose)
}

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (session *Session) changeChannel(channel *amqp.Channel) {
	session.channel = channel
	session.notifyChanClose = make(chan *amqp.Error)
	session.notifyConfirm = make(chan amqp.Confirmation, 1)
	session.channel.NotifyClose(session.notifyChanClose)
	session.channel.NotifyPublish(session.notifyConfirm)
}

// Push will push data onto the queue, and wait for a confirm.
// If no confirms are received until within the resendTimeout,
// it continuously re-sends messages until a confirm is received.
// This will block until the server sends a confirm. Errors are
// only returned if the push action itself fails, see UnsafePush.
func (session *Session) Push(data []byte) error {
	if !session.isReady {
		return errors.New("failed to push push: not connected")
	}
	for {
		err := session.UnsafePush(data)
		if err != nil {
			session.logger.Errorf("failed to UnsafePush: %s", err)
			select {
			case <-session.done:
				return ErrShutdown
			case <-time.After(resendDelay):
			}
			continue
		}
		select {
		case confirm := <-session.notifyConfirm:
			if confirm.Ack {
				return nil
			}
		case <-time.After(resendDelay):
		}
	}
}

// UnsafePush will push to the queue without checking for
// confirmation. It returns an error if it fails to connect.
// No guarantees are provided for whether the server will
// recieve the message.
func (session *Session) UnsafePush(data []byte) error {
	if !session.isReady {
		return ErrNotConnected
	}
	return session.channel.Publish(
		session.exchangeName, // Exchange
		session.bindingKey,   // Routing key
		false,                // Mandatory
		false,                // Immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent,
			Body:         data,
		},
	)
}

// Stream will continuously put queue items on the channel.
// It is required to call delivery.Ack when it has been
// successfully processed, or delivery.Nack when it fails.
// Ignoring this will cause data to build up on the server.
func (session *Session) Stream() (<-chan amqp.Delivery, error) {
	if !session.isReady {
		return nil, ErrNotConnected
	}
	return session.channel.Consume(
		session.queueName,
		session.consumerTag, // Consumer
		false,               // Auto-Ack
		false,               // Exclusive
		false,               // No-local
		false,               // No-Wait
		nil,                 // Args
	)
}

func (session *Session) Subscribe(handler func([]byte) error) {
	go func() {
		for {
			deliveries, err := session.Stream()
			if err == ErrNotConnected {
				session.logger.Errorf("failed to Stream: %s", err)
				time.Sleep(reInitDelay)
				continue
			} else if err != nil {
				// Panic!
				session.logger.Panic(err)
			}

			session.logger.Info("Consumer delivers!")

			for deliveries != nil {
				select {
				case d, ok := <-deliveries:
					{
						if !ok {
							deliveries = nil
							continue
						}
						session.handleDelivery(d, handler)
					}
				case <-session.NotifyClose():
					deliveries = nil
				}
			}

			session.logger.Info("Consumer closed!")
		}
	}()
}

func (session *Session) handleDelivery(d amqp.Delivery, handler func([]byte) error) {
	msgId := d.MessageId
	if msgId != "" && session.msgSeen.Get(msgId) {
		d.Ack(false)
		session.logger.Errorw("Message with this msgId was already processed", "msgId", msgId)
		return
	}
	session.logger.Infow("Start processing message", "msgId", msgId)

	defer func() {
		if r := recover(); r != nil {
			session.logger.Errorw("panic", r, "msgId", msgId)
			_ = d.Nack(false, true)
		}
	}()

	if err := handler(d.Body); err == nil {
		if err := d.Ack(false); err != nil {
			session.logger.Errorw("failed to Ack", err, "msgId", msgId)
		} else {
			session.msgSeen.Set(msgId)
			session.logger.Infow("Message processed successfully", "msgId", msgId)
		}
	} else {
		session.logger.Errorw("failed to handleDelivery", err, "msgId", msgId)
		_ = d.Nack(false, true)
	}
}

// Close will cleanly shutdown the channel and connection.
func (session *Session) Close() error {
	if !session.isReady {
		return ErrAlreadyClosed
	}

	session.setState(false)
	err := session.channel.Close()
	if err != nil {
		return fmt.Errorf("failed to channel close: %w", err)
	}
	err = session.connection.Close()
	if err != nil {
		return fmt.Errorf("failed to connection close: %w", err)
	}
	close(session.done)

	return nil
}
