// Package msgpack adds to the message.Request a conversion to msgpack and vice-versa.
package msgpack

import (
	"bytes"
	"fmt"
	"github.com/ahmetson/datatype-lib/message"
	"github.com/vmihailenco/msgpack/v5"
)

type Request struct {
	*message.Request
}

type Reply struct {
	*message.Reply
}

// Message returns a message for parsing request and parsing reply from msgpack format.
func Message() *message.Operations {
	return &message.Operations{
		Name:       "msgpack",
		NewReq:     NewReq,
		NewReply:   NewReply,
		EmptyReq:   NewEmptyReq,
		EmptyReply: NewEmptyReply,
	}
}

func NewEmptyReq() message.RequestInterface {
	return &Request{}
}

func NewEmptyReply() message.ReplyInterface {
	return &Reply{}
}

// NewReq returns a message pack from the zeromq message envelope.
func NewReq(messages []string) (message.RequestInterface, error) {
	msg := message.JoinMessages(messages)
	msgBuf := bytes.NewReader([]byte(msg))
	var request message.Request

	dec := msgpack.NewDecoder(msgBuf)
	dec.SetCustomStructTag("json")

	err := dec.Decode(&request)
	if err != nil {
		return nil, fmt.Errorf("msgpack.Decoder.Decode: %w", err)
	}

	req := &Request{
		&request,
	}

	return req, nil
}

// NewReply returns a message pack from the zeromq message envelope.
func NewReply(messages []string) (message.ReplyInterface, error) {
	msg := message.JoinMessages(messages)
	msgBuf := bytes.NewReader([]byte(msg))
	var reply message.Reply

	dec := msgpack.NewDecoder(msgBuf)
	dec.SetCustomStructTag("json")

	err := dec.Decode(&reply)
	if err != nil {
		return nil, fmt.Errorf("msgpack.Decoder.Decode: %w", err)
	}

	rep := &Reply{
		&reply,
	}

	return rep, nil
}

//
// Request methods
//

// Bytes convert the message to the sequence of bytes
func (request *Request) Bytes() ([]byte, error) {
	err := message.ValidCommand(request.Command)
	if err != nil {
		return nil, fmt.Errorf("failed to validate command: %w", err)
	}

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.SetCustomStructTag("json")

	err = enc.Encode(request)
	if err != nil {
		return nil, fmt.Errorf("msgpack.Encoder.Encode: %w", err)
	}

	return buf.Bytes(), nil
}

// String representation of the msgpack
func (request *Request) String() string {
	reqBytes, err := request.Bytes()
	if err != nil {
		return ""
	}

	return string(reqBytes)
}

func (request *Request) ZmqEnvelope() ([]string, error) {
	str := request.String()

	if len(request.ConId()) > 0 {
		return []string{request.ConId(), "", str}, nil
	}

	return []string{"", str}, nil
}

//
// Reply methods
//

// Bytes converts Reply to the sequence of bytes
func (reply *Reply) Bytes() ([]byte, error) {
	err := message.ValidFail(reply.Status, reply.Message)
	if err != nil {
		return nil, fmt.Errorf("failure validation: %w", err)
	}
	err = message.ValidStatus(reply.Status)
	if err != nil {
		return nil, fmt.Errorf("status validation: %w", err)
	}

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.SetCustomStructTag("json")

	err = enc.Encode(reply)
	if err != nil {
		return nil, fmt.Errorf("msgpack.Encoder.Encode: %w", err)
	}

	return buf.Bytes(), nil
}

// String converts the Reply to the string format
func (reply *Reply) String() string {
	reqBytes, err := reply.Bytes()
	if err != nil {
		return ""
	}

	return string(reqBytes)
}

func (reply *Reply) ZmqEnvelope() ([]string, error) {
	reqBytes, err := reply.Bytes()
	if err != nil {
		return nil, fmt.Errorf("request.ZmqEnvelope: %w", err)
	}

	str := string(reqBytes)

	if len(reply.ConId()) > 0 {
		return []string{reply.ConId(), "", str}, nil
	}

	return []string{"", str}, nil
}
