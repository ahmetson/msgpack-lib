package msgpack

import (
	"bytes"
	"github.com/ahmetson/datatype-lib/message"
	"github.com/vmihailenco/msgpack/v5"
	"testing"

	"github.com/ahmetson/datatype-lib/data_type/key_value"
	"github.com/stretchr/testify/suite"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing orchestra
type TestMsgPackSuite struct {
	suite.Suite

	cmdName    string
	request    message.RequestInterface
	reqMsgPack string
	reqMsg     []string

	reply      message.ReplyInterface
	repMsgPack string
	repMsg     []string

	key   string
	value uint64
}

// Uses the following tool to generate the text
// https://msgpack.solder.party/
func (test *TestMsgPackSuite) SetupTest() {
	s := test.Require

	test.cmdName = "command"
	test.key = "number"
	test.value = 123

	test.request = &message.Request{
		Command:    test.cmdName,
		Parameters: key_value.New(),
	}
	var reqBuf bytes.Buffer
	enc := msgpack.NewEncoder(&reqBuf)
	enc.SetCustomStructTag("json")

	err := enc.Encode(test.request)
	s().NoError(err)

	test.reqMsgPack = string(reqBuf.Bytes())
	test.reqMsg = []string{"req_id", "", test.reqMsgPack}

	test.reply = &message.Reply{
		Status:     message.OK,
		Message:    "",
		Parameters: key_value.New().Set(test.key, test.value),
	}
	var repBuf bytes.Buffer
	enc = msgpack.NewEncoder(&repBuf)
	enc.SetCustomStructTag("json")

	err = enc.Encode(test.reply)
	s().NoError(err)
	test.repMsgPack = repBuf.String()
	test.repMsg = []string{"req_id", "", test.repMsgPack}
}

// Test_10_Message checks that operations are returned
func (test *TestMsgPackSuite) Test_10_Message() {
	s := test.Require

	messageOps := Message()

	s().Equal("msgpack", messageOps.Name)

	expected, err := NewReq(test.reqMsg)
	s().NoError(err)
	actual, err := messageOps.NewReq(test.reqMsg)
	s().NoError(err)
	s().EqualValues(expected, actual)

	expectedReply, err := NewReply(test.repMsg)
	s().NoError(err)
	actualReply, err := messageOps.NewReply(test.repMsg)
	s().NoError(err)
	s().EqualValues(expectedReply, actualReply)

	s().Empty(messageOps.EmptyReq())
	s().Empty(messageOps.EmptyReply())
}

// Test_11_NewReq tests converting of the zeromq message envelope into Request
func (test *TestMsgPackSuite) Test_11_NewReq() {
	s := test.Require

	// non multipart message and non sync replier envelope must fail
	_, err := NewReq(test.reqMsg[:1])
	s().Error(err)

	// trying to convert an invalid message.Request must fail
	_, err = NewReq([]string{"", "", ""})
	s().Error(err)

	// without a stack
	rawInterface, err := NewReq(test.reqMsg)
	s().NoError(err)
	s().True(rawInterface.IsFirst())
	s().NotEmpty(rawInterface.ConId())

	// sync replier message must be successful too
	rawInterface, err = NewReq(test.reqMsg[1:])
	s().NoError(err)
	s().True(rawInterface.IsFirst())
	s().Empty(rawInterface.ConId())
}

// Test_12_NewReply tests converting of the zeromq message envelope into Reply
func (test *TestMsgPackSuite) Test_12_NewReply() {
	s := test.Require

	// non multipart message and non sync replier envelope must fail
	_, err := NewReply(test.repMsg[:1])
	s().Error(err)

	// trying to convert an invalid message.Request must fail
	_, err = NewReply([]string{"", "", ""})
	s().Error(err)

	// without a stack
	rawInterface, err := NewReply(test.repMsg)
	s().NoError(err)
	s().True(rawInterface.IsOK())
	parameters := rawInterface.ReplyParameters()
	value, err := parameters.Uint64Value(test.key)
	s().NoError(err)
	s().Equal(test.value, value)
	s().NotEmpty(rawInterface.ConId())

	// for a sync replier
	rawInterface, err = NewReply(test.repMsg[1:])
	s().NoError(err)
	s().True(rawInterface.IsOK())
	parameters = rawInterface.ReplyParameters()
	value, err = parameters.Uint64Value(test.key)
	s().NoError(err)
	s().Equal(test.value, value)
	s().Empty(rawInterface.ConId())
}

// Test_13_Bytes tests conversion of the message to the bytes.
// Tests Request and Reply.
//
// IMPORTANT. No need to test for String, since string converts Bytes
func (test *TestMsgPackSuite) Test_13_Bytes() {
	s := test.Require

	// request
	req, err := NewReq(test.reqMsg)
	s().NoError(err)
	reqBytes, err := req.Bytes()
	s().NoError(err)
	s().Equal([]byte(test.reqMsgPack), reqBytes)

	// reply
	reply, err := NewReply(test.repMsg)
	s().NoError(err)
	replyBytes, err := reply.Bytes()
	s().NoError(err)
	s().Equal([]byte(test.repMsgPack), replyBytes)
}

// Test_14_ZmqEnvelope tests conversion of the message to the zmq envelope.
// Tests Request and Reply.
func (test *TestMsgPackSuite) Test_14_ZmqEnvelope() {
	s := test.Require

	// Request
	req, err := NewReq(test.reqMsg)
	s().NoError(err)
	zmqEnvelope, err := req.ZmqEnvelope()
	s().NoError(err)
	s().Equal(test.reqMsg, zmqEnvelope)

	// Sync replier envelope
	req, err = NewReq(test.reqMsg[1:])
	s().NoError(err)
	zmqEnvelope, err = req.ZmqEnvelope()
	s().NoError(err)
	s().Equal(test.reqMsg[1:], zmqEnvelope)

	// Reply
	reply, err := NewReply(test.repMsg)
	s().NoError(err)
	zmqEnvelope, err = reply.ZmqEnvelope()
	s().NoError(err)
	s().Equal(test.repMsg, zmqEnvelope)

	// Sync replier
	reply, err = NewReply(test.repMsg[1:])
	s().NoError(err)
	zmqEnvelope, err = reply.ZmqEnvelope()
	s().NoError(err)
	s().Equal(test.repMsg[1:], zmqEnvelope)
}

func TestMsgPack(t *testing.T) {
	suite.Run(t, new(TestMsgPackSuite))
}
