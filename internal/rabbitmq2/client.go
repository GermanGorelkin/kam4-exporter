package rabbitmq2

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
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
	errNotConnected  = errors.New("not connected to a server")
	errAlreadyClosed = errors.New("already closed: not connected to the server")
	errShutdown      = errors.New("client is shutting down")
)

// Client is the base struct for handling connection recovery, consumption and
// publishing. Note that this struct has an internal mutex to safeguard against
// data races. As you develop and iterate over this example, you may need to add
// further locks, or safeguards, to keep your application safe from data races
type Client struct {
	exchangeName string
	exchangeType string
	queueName    string
	bindingKey   string
	consumerTag  string

	m               *sync.Mutex
	connection      *amqp.Connection
	channel         *amqp.Channel
	done            chan bool
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	isReady         bool

	logger *zap.SugaredLogger
}

type ClientConfig struct {
	ExchangeName string
	ExchangeType string
	QueueName    string
	BindingKey   string
	Addr         string
	Logger       *zap.SugaredLogger
}

// New creates a new consumer state instance, and automatically
// attempts to connect to the server.
func New(cfg ClientConfig) *Client {
	client := Client{
		exchangeName: cfg.ExchangeName,
		exchangeType: cfg.ExchangeType,
		queueName:    cfg.QueueName,
		bindingKey:   cfg.BindingKey,

		m:    &sync.Mutex{},
		done: make(chan bool),

		logger: cfg.Logger,
	}
	go client.handleReconnect(cfg.Addr)
	return &client
}

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (client *Client) handleReconnect(addr string) {
	for {
		client.m.Lock()
		client.isReady = false
		client.m.Unlock()

		client.logger.Info("attempting to connect")

		conn, err := client.connect(addr)
		if err != nil {
			client.logger.Errorf("failed to connect: %s", err)
			client.logger.Info("Retrying...")

			select {
			case <-client.done:
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}

		if done := client.handleReInit(conn); done {
			break
		}
	}
}

// connect will create a new AMQP connection
func (client *Client) connect(addr string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(addr)
	if err != nil {
		err = fmt.Errorf("failed to dial to %s: %w", addr, err)
		return nil, err
	}

	client.changeConnection(conn)
	client.logger.Info("Connected!")
	return conn, nil
}

// handleReInit will wait for a channel error
// and then continuously attempt to re-initialize both channels
func (client *Client) handleReInit(conn *amqp.Connection) bool {
	for {
		client.m.Lock()
		client.isReady = false
		client.m.Unlock()

		err := client.init(conn)
		if err != nil {
			client.logger.Errorf("failed to initialize channel: %s", err)
			client.logger.Info("Retrying...")

			select {
			case <-client.done:
				return true
			case <-client.notifyConnClose:
				client.logger.Errorf("connection closed, reconnecting...")
				return false
			case <-time.After(reInitDelay):
			}
			continue
		}

		select {
		case <-client.done:
			return true
		case <-client.notifyConnClose:
			client.logger.Errorf("connection closed, reconnecting...")
			return false
		case <-client.notifyChanClose:
			client.logger.Errorf("channel closed, re-running init...")
		}
	}
}

// init will initialize channel & declare queue
func (client *Client) init(conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to conn Channel: %w", err)
	}

	err = ch.Confirm(false)
	if err != nil {
		return fmt.Errorf("failed to Confirm: %w", err)
	}

	err = ch.ExchangeDeclare(
		client.exchangeName, // name
		client.exchangeType, // type
		false,               // durable
		false,               // auto-deleted
		false,               // internal
		false,               // no-wait
		nil,                 // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to ExchangeDeclare: %w", err)
	}

	_, err = ch.QueueDeclare(
		client.queueName,
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
		client.queueName,
		client.bindingKey,
		client.exchangeName,
		false,
		nil)
	if err != nil {
		return fmt.Errorf("failed to QueueBind: %w", err)
	}

	err = ch.Qos(1, 0, false)
	if err != nil {
		return fmt.Errorf("failed to Qos: %w", err)
	}

	client.changeChannel(ch)
	client.m.Lock()
	client.isReady = true
	client.m.Unlock()
	client.logger.Info("Setup!")

	return nil
}

// changeConnection takes a new connection to the queue,
// and updates the close listener to reflect this.
func (client *Client) changeConnection(connection *amqp.Connection) {
	client.connection = connection
	client.notifyConnClose = make(chan *amqp.Error, 1)
	client.connection.NotifyClose(client.notifyConnClose)
}

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (client *Client) changeChannel(channel *amqp.Channel) {
	client.channel = channel
	client.notifyChanClose = make(chan *amqp.Error, 1)
	client.notifyConfirm = make(chan amqp.Confirmation, 1)
	client.channel.NotifyClose(client.notifyChanClose)
	client.channel.NotifyPublish(client.notifyConfirm)
}

// Push will push data onto the queue, and wait for a confirmation.
// This will block until the server sends a confirmation. Errors are
// only returned if the push action itself fails, see UnsafePush.
func (client *Client) Push(data []byte) error {
	client.m.Lock()
	if !client.isReady {
		client.m.Unlock()
		return errNotConnected
	}
	client.m.Unlock()
	for {
		err := client.UnsafePush(data)
		if err != nil {
			client.logger.Errorf("push failed. Retrying...")
			select {
			case <-client.done:
				return errShutdown
			case <-time.After(resendDelay):
			}
			continue
		}
		confirm := <-client.notifyConfirm
		if confirm.Ack {
			client.logger.Errorf("push confirmed [%d]", confirm.DeliveryTag)
			return nil
		}
	}
}

// UnsafePush will push to the queue without checking for
// confirmation. It returns an error if it fails to connect.
// No guarantees are provided for whether the server will
// receive the message.
func (client *Client) UnsafePush(data []byte) error {
	client.m.Lock()
	if !client.isReady {
		client.m.Unlock()
		return errNotConnected
	}
	client.m.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return client.channel.PublishWithContext(
		ctx,
		client.exchangeName, // Exchange
		client.bindingKey,   // Routing key
		false,               // Mandatory
		false,               // Immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent,
			Body:         data,
		},
	)
}

// Consume will continuously put queue items on the channel.
// It is required to call delivery.Ack when it has been
// successfully processed, or delivery.Nack when it fails.
// Ignoring this will cause data to build up on the server.
func (client *Client) Consume() (<-chan amqp.Delivery, error) {
	client.m.Lock()
	if !client.isReady {
		client.m.Unlock()
		return nil, errNotConnected
	}
	client.m.Unlock()

	if err := client.channel.Qos(
		1,     // prefetchCount
		0,     // prefetchSize
		false, // global
	); err != nil {
		return nil, err
	}

	return client.channel.Consume(
		client.queueName,
		client.consumerTag, // Consumer
		false,              // Auto-Ack
		false,              // Exclusive
		false,              // No-local
		false,              // No-Wait
		nil,                // Args
	)
}

// Close will cleanly shut down the channel and connection.
func (client *Client) Close() error {
	client.m.Lock()
	// we read and write isReady in two locations, so we grab the lock and hold onto
	// it until we are finished
	defer client.m.Unlock()

	if !client.isReady {
		return errAlreadyClosed
	}
	close(client.done)
	err := client.channel.Close()
	if err != nil {
		return fmt.Errorf("failed to channel close: %w", err)
	}
	err = client.connection.Close()
	if err != nil {
		return fmt.Errorf("failed to connection close: %w", err)
	}

	client.isReady = false
	return nil
}

func (client *Client) Subscribe(handler func([]byte) error) {
	go func() {
		deliveries, err := client.Consume()
		if err != nil {
			client.logger.Errorf("could not start consuming: %s\n", err)
			return
		}

		chClosedCh := make(chan *amqp.Error, 1)
		client.channel.NotifyClose(chClosedCh)

		client.logger.Info("Consumer delivers!")

		for {
			select {
			case amqErr := <-chClosedCh:
				// This case handles the event of closed channel e.g. abnormal shutdown
				client.logger.Errorf("AMQP Channel closed due to: %s\n", amqErr)

				deliveries, err = client.Consume()
				if err != nil {
					client.logger.Errorf("error trying to consume, will try again")
					continue
				}

				// Re-set channel to receive notifications
				// The library closes this channel after abnormal shutdown
				chClosedCh = make(chan *amqp.Error, 1)
				client.channel.NotifyClose(chClosedCh)

			case delivery := <-deliveries:
				client.handleDelivery(delivery, handler)
			}
		}
	}()
}

func (client *Client) handleDelivery(d amqp.Delivery, handler func([]byte) error) {
	defer func() {
		if r := recover(); r != nil {
			client.logger.Errorf("Handler panic:%v", r)
			_ = d.Nack(false, true)
		}
	}()

	if err := handler(d.Body); err == nil {
		if err := d.Ack(false); err != nil {
			client.logger.Errorf("failed to Ack: %s", err)
		}
	} else {
		client.logger.Errorf("failed to handleDelivery: %s", err)
		_ = d.Nack(false, true)
	}
}
