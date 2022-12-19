package message

import (
	"os"
	"os/user"
	"time"

	"github.com/JinishBhardwaj/messagebus-go/message"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

// New creates a TcWire message
func New(provider string, messageType string, payload []byte, reply_to string, correlation_id string) (msg *message.TcWire) {

	now := time.Now()
	hostname, _ := os.Hostname()
	user, _ := user.Current()

	msg_id := uuid.New().String()

	m := message.TcWire{
		Id:            msg_id,
		Provider:      provider,
		CreatedTs:     now.UnixNano(),
		MessageType:   messageType,
		Payload:       payload,
		Hostname:      hostname,
		ProcessId:     int32(os.Getpid()),
		GeneratedBy:   user.Username,
		ReplyTo:       reply_to,
		CorrelationId: correlation_id,
	}

	msg = &m

	return
}

func FromBytes(b []byte) (w *message.TcWire, err error) {
	msg := new(message.TcWire)
	err = proto.Unmarshal(b, msg)
	w = msg
	return
}
