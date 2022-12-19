package mercury

import (
	"context"

	"github.com/JinishBhardwaj/messagebus-go/message"

	"google.golang.org/protobuf/proto"
)

// MessageBus contains the interface definition to
// support any Message Bus implementation
type MessageBus interface {
	Send(ctx context.Context, dst string, msg *message.TcWire) error
	Ack(ctx context.Context, id string, isNack bool)
	Messages() <-chan *message.TcWire
	Close()
	GetReplyToAddress() string
	Call(string, proto.Message, string) (string, error)
	CountInFlight() int
}
