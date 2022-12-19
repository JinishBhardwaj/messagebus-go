package mercury

import (
	"context"
	"strconv"
	"sync"

	"github.com/JinishBhardwaj/messagebus-go/message"

	wire "github.com/JinishBhardwaj/messagebus-go/pkg/message"

	"github.com/pkg/errors"

	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

// this is how many messages to keep in memory
const buffer_size = 100

// RabbitMQ is a structure that contains the amqp.Connection
// the Topics that we're subscribing to, and the name of the
// Exchange to connect to
type RabbitMQ struct {
	Ctx                context.Context
	Conn               *amqp.Connection
	Exchange           string
	RawMessages        <-chan amqp.Delivery
	PrivateRawMessages <-chan amqp.Delivery
	WireMessages       chan *message.TcWire
	replyToAddress     string
}

var ackMap sync.Map

func (rmq *RabbitMQ) Messages() (ch <-chan *message.TcWire) {
	return rmq.WireMessages
}

func (rmq *RabbitMQ) setToAckMap(id string, payload amqp.Delivery) {
	ackMap.Store(id, payload)
}

func (rmq *RabbitMQ) getFromAckMap(id string) (payload amqp.Delivery, err error) {

	result, ok := ackMap.LoadAndDelete(id)

	if ok {
		payload = result.(amqp.Delivery)
	} else {
		err = errors.New("not found")
	}

	return
}

func (rmq *RabbitMQ) deleteFromAckMap(id string) {
	ackMap.Delete(id)
}

func (rmq *RabbitMQ) Send(ctx context.Context, destination string, msg *message.TcWire) (err error) {

	// open a new channel
	ch, err := rmq.Conn.Channel()

	if err != nil {
		log.Errorf("Send() error: %v", err)
		return
	}

	defer ch.Close()

	raw_msg, _ := proto.Marshal(msg)

	err = ch.PublishWithContext(
		ctx,          // context
		rmq.Exchange, // exchange
		destination,  // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			Body:          raw_msg,
			CorrelationId: msg.GetCorrelationId(),
			ReplyTo:       msg.GetReplyTo(),
			Expiration:    strconv.FormatInt(int64(msg.GetExpiresInMs()), 10),
		})

	if err != nil {
		log.Infof("error sending message: %v", err)
	}

	return
}

func (rmq *RabbitMQ) Close() {
	rmq.Conn.Close()
}

// NewRabbitMQ creates a new instance of RabbitMQ
func NewRabbitMQ(ctx context.Context, url string, queue string, exchange string, consumer string) (rmq RabbitMQ, err error) {

	conn, err := amqp.Dial(url)

	if err != nil {
		return
	}

	logger.Debugf("connected to rabbitmq: %s", url)

	rmq = RabbitMQ{
		Conn:     conn,
		Exchange: exchange,
	}

	ch, err := conn.Channel()
	if err != nil {
		return
	}

	logger.Debug("channel was opened")

	err = ch.ExchangeDeclare(
		exchange, // name
		"direct", // type
		false,    // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		return
	}

	logger.Debugf("exchange '%s' declared (direct)", exchange)

	var service_queue amqp.Queue

	if queue != "" {

		service_queue, err = ch.QueueDeclare(
			queue, // name
			false, // durable
			false, // delete when usused
			false, // exclusive
			false, // no-wait
			nil,   // arguments
		)
		if err != nil {
			return
		}

		// bind the queue to the exchange
		err = ch.QueueBind(
			service_queue.Name, // queue name
			service_queue.Name, // routing key
			exchange,           // exchange
			false,
			nil,
		)

		if err != nil {
			logger.Errorln(errors.Wrap(err, "cannot bind service queue to exchange"))
			return
		}

		logger.Debugf("service queue (%s) declared", service_queue.Name)

	}

	// private queue
	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when usused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		logger.Errorln(errors.Wrap(err, "error declaring private queue"))
		return
	}

	logger.Debugf("private queue (%s) declared", q.Name)

	rmq.replyToAddress = q.Name

	// bind the queue to the exchange
	err = ch.QueueBind(
		q.Name,   // queue name
		q.Name,   // routing key
		exchange, // exchange
		false,
		nil,
	)

	if err != nil {
		logger.Errorf("cannot bind queue %v, to queue: %v, in exchange: %v", q.Name, queue, exchange)
	}

	private_msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto ack
		true,   // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	if err != nil {
		logger.Errorln(errors.Wrap(err, "cannot consume from private messages"))
		return
	}

	var msgs <-chan amqp.Delivery

	if queue != "" {
		msgs, err = ch.Consume(
			service_queue.Name, // queue
			"",                 // consumer
			false,              // auto ack
			false,              // exclusive
			false,              // no local
			false,              // no wait
			nil,                // args
		)
		if err != nil {
			logger.Errorln(errors.Wrap(err, "cannot consume from service messages"))
			return
		}
	}

	logger.Debugf("initializing message consumption")

	if queue != "" {
		rmq.RawMessages = msgs
	}

	rmq.PrivateRawMessages = private_msgs

	outputChannel := make(chan *message.TcWire, buffer_size)
	rmq.WireMessages = outputChannel

	go rmq.Receive(ctx)

	return
}

func (rmq *RabbitMQ) Receive(ctx context.Context) {
	logger.Infof("RabbitMQ receiver started")
	for {
		select {
		case d := <-rmq.RawMessages:

			rmq.DecodeAndProcess(d)

		case d := <-rmq.PrivateRawMessages:

			rmq.DecodeAndProcess(d)

		case <-ctx.Done():
			logger.Debugf("RabbitMQ receiver: got termination signal")
			return
		}
	}
}

func (rmq *RabbitMQ) DecodeAndProcess(d amqp.Delivery) {
	msg, err := wire.FromBytes(d.Body)

	if err != nil {
		logger.Errorln(errors.Wrap(err, "RabbitMQ receiver: cannot decode payload"))
		return
	}

	rmq.setToAckMap(msg.GetId(), d)

	rmq.WireMessages <- msg
}

func (rmq *RabbitMQ) GetReplyToAddress() string {
	return rmq.replyToAddress
}

// Ack acknowledges a message that has been successfully
// processed
func (rmq *RabbitMQ) Ack(ctx context.Context, id string, isNack bool) {

	msg, err := rmq.getFromAckMap(id)

	if err != nil {
		logger.Errorf("cannot find id: %s in to-be acked list: %v", id, err)
		return
	}

	if isNack {
		msg.Nack(false, false)
	} else {
		msg.Ack(false)
	}

	rmq.deleteFromAckMap(id)

}

var inFlight sync.Map

func (rmq *RabbitMQ) SetToInFlight(id string, payload bool) {
	inFlight.Store(id, payload)
}

func (rmq *RabbitMQ) GetFromInFlight(id string) (present bool) {

	_, ok := inFlight.Load(id)

	if ok {
		present = true
	}

	return
}

func (rmq *RabbitMQ) DeleteFromInFlight(id string) {
	inFlight.Delete(id)
}

func (rmq *RabbitMQ) CountInFlight() (total int) {

	inFlight.Range(func(k any, v any) bool {
		total++
		return true
	})

	return

}

func (rmq *RabbitMQ) Call(service string, msg proto.Message, msg_type string) (msg_id string, err error) {

	payload, err := proto.Marshal(msg)

	if err != nil {
		return
	}

	wire_msg := wire.New(
		service,
		msg_type,
		payload,
		rmq.GetReplyToAddress(),
		"",
	)

	rmq.SetToInFlight(wire_msg.GetId(), true)
	rmq.Send(context.Background(), service, wire_msg)

	msg_id = wire_msg.GetId()

	return

}
