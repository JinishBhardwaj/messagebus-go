package mercury

import (
	"context"
	"os"
	"reflect"
	"sync"

	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

var logger *log.Entry

func init() {

	log.SetOutput(os.Stdout)

	logger = log.WithFields(log.Fields{"ctx": os.Args[0]})

	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})
	log.SetLevel(log.DebugLevel)
}

type HandlerFuncType func(*Server, proto.Message) (err error)

// HandlerType this is the handler to be used when registering a new
// service
type HandlerType struct {
	Handler     HandlerFuncType // callback function to call with payload
	MessageType interface{}     // this holds a variable of the expected message type
}

// Mercury is the main structure that consumes messages and invokes
// call to the Handler Function, implemented by a micro-service
type Mercury struct {
	MessageBus  *RabbitMQ
	CancelCtx   func()
	Ctx         context.Context
	wg          sync.WaitGroup
	numReaders  int
	Handlers    map[string]HandlerType
	Consuming   chan bool
	ServiceName string
}

// New creates a new Mercury
func New(url string, serviceName string, exchange string, numReaders int) (m *Mercury, err error) {

	m = new(Mercury)

	// this will be attached to messages going out as well as the name
	// of the queue/topic for consuming
	m.ServiceName = serviceName

	ctx := context.Background()
	ctx, cancelCtx := context.WithCancel(ctx)
	m.CancelCtx = cancelCtx

	// attach the context
	m.Ctx = ctx

	rmq, err := NewRabbitMQ(ctx, url, serviceName, exchange, serviceName)
	if err != nil {
		return
	}

	m.MessageBus = &rmq

	consuming := make(chan bool, 1)
	m.Consuming = consuming

	if numReaders == 0 {
		numReaders = 1
	}

	m.numReaders = numReaders

	for i := 1; i <= numReaders; i++ {
		m.wg.Add(1)
		go m.receive(ctx, i)
	}

	m.Handlers = make(map[string]HandlerType, 1)
	log.Infof("ready to consume messages")

	return
}

// Register is used to add a handler to Mercury
func (m *Mercury) Register(msg_type string, f HandlerFuncType, t proto.Message) {
	m.Handlers[msg_type] = HandlerType{
		Handler:     f,
		MessageType: t,
	}
}

func (m *Mercury) WaitForResponses() {
	var i int
	for m.MessageBus.CountInFlight() > 0 {
		if (i % 1000) == 0 {
			logger.Debugf("waiting for responses; left: %d", m.MessageBus.CountInFlight())
		}
		i++
	}
}

// Finalize is used when cleaning up
func (m *Mercury) Finalize() {

	logger.Debug("Finalize() sending terminate signal")

	// this will trigger cancelations down the stream
	m.CancelCtx()

	logger.Debug("waiting for readers to terminate")
	m.wg.Wait()

	logger.Debug("closing MessageBus connection")
	m.MessageBus.Close()

	close(m.Consuming)

	logger.Debug("Done Finalizing")

}

// Receive starts reading from rabbitMQ and sends the
// resulting wire.TcWire to the ouput channel
func (m *Mercury) receive(ctx context.Context, i int) {

	defer m.wg.Done()

	logger.Debugf("Mercury[%d] receiving messages", i)

	service_msg_ch := m.MessageBus.Messages()

	for {
		select {
		case msg := <-service_msg_ch:

			// get the message type
			messageType := msg.GetMessageType()

			// if there are no handlers, no need to do anything
			if m.Handlers == nil {
				continue
			}

			// get the handler from the registered types
			handler, ok := m.Handlers[messageType]

			if !ok {
				logger.Errorf("Received unsupported message: %s", messageType)
				m.MessageBus.Ack(ctx, msg.Id, true)
				continue
			}

			// check to see if this message is a response

			msg_correlation_id := msg.GetCorrelationId()
			total_in_flight := m.MessageBus.CountInFlight()

			if msg_correlation_id != "" && (total_in_flight > 0) {

				ok := m.MessageBus.GetFromInFlight(msg_correlation_id)

				if !ok {
					logger.Errorf("Received unexpected message (incorrect correlation_id): %v -- MSG: %s", msg_correlation_id, msg.String())
					continue
				} else {
					// logger.Debugf("received a valid response: %v", msg_correlation_id)
					m.MessageBus.DeleteFromInFlight(msg_correlation_id)
				}
			}

			// delivery_info is used to obtain the message properties, it could
			// also be passed as part of the Server structure, this will be
			// refactored later
			// ctx = context.WithValue(ctx, "delivery_info", d)

			server_ctx := Server{
				Ctx:        ctx,
				MessageBus: m.MessageBus,
				Envelope:   msg,
				Provider:   m.ServiceName,
			}

			// this is the interface{} that contains the underlying type
			// to be used to decode the message
			payloadDecodeWith := handler.MessageType

			// we extract the underlying type (reflect.Type)
			mt := reflect.TypeOf(payloadDecodeWith)

			// with the reflect.Type we create a new instance
			// reflect.New() initialized memory and returns a pointer type
			// which is still a reflect type.
			payloadReflect := reflect.New(mt.Elem())

			// we obtain the interface{} type of the newly created variable
			// and cast to proto.Message which is the generic interface type
			payload := payloadReflect.Interface().(proto.Message)

			// we call unmarshal on the payload
			err := proto.Unmarshal(msg.GetPayload(), payload)

			if err != nil {
				logger.Errorf("error decoding message of type: %s: %v", messageType, err)
				continue
			}

			// we call the handler function
			func() {
				err := handler.Handler(&server_ctx, payload)
				if err != nil {
					logger.Errorf("call to handler of %s was not successful: %v, not acking mesage", messageType, err)
					m.MessageBus.Ack(ctx, msg.Id, true)
				} else {
					m.MessageBus.Ack(ctx, msg.Id, false)
				}
			}()

		case <-ctx.Done():
			logger.Debugf("Mercury[%d]: got termination signal", i)
			return
		}
	}

}

func (m *Mercury) WaitUntilComplete() {
	<-m.Consuming
}

func (m *Mercury) Call(service string, msg proto.Message, msg_type string) (msg_id string, err error) {

	return m.MessageBus.Call(service, msg, msg_type)

}
