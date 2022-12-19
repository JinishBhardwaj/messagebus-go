package mercury

import (
	"context"

	"github.com/JinishBhardwaj/messagebus-go/message"
	wire "github.com/JinishBhardwaj/messagebus-go/pkg/message"

	"google.golang.org/protobuf/proto"
)

type Server struct {
	Ctx        context.Context
	MessageBus MessageBus
	Envelope   *message.TcWire
	Provider   string // name of this consumer
}

func (s *Server) Send(destination string, reply_to string, msg_type string, msg proto.Message, correlation_id string) (err error) {

	payload, _ := proto.Marshal(msg)

	wire_msg := wire.New(
		s.Provider,
		msg_type,
		payload,
		reply_to,
		correlation_id,
	)

	logger.Infof("Sending message to: %s / type %s", destination, msg_type)

	// will need to replace with a MessageBus abstraction method
	return s.MessageBus.Send(s.Ctx, destination, wire_msg)
}

func (s *Server) Reply(msg_type string, msg proto.Message) (err error) {
	s.Send(s.Envelope.GetReplyTo(), "", msg_type, msg, s.Envelope.GetId())
	return
}
