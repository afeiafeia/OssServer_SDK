//自定义协议报文的编解码器
package oss

import (
	"bytes"
	"reflect"

	"github.com/rangechow/errors"
)

const MAX_CMD_LEN = 32
const HEADER = "header"
const MAX_TASKID = 128

const (
	MAGIC_NUM_ERROR errors.ErrCode = iota
	LENGTH_ERROR
	BUFFER_LT_HEADER
	DECODE_HEADER_ERROR
	BUFFER_LT_PAYLOAD
)

type Header interface {
	GetMagic() int
	SetMagic(m int)

	GetMsgId() uint32
	SetMsgId(i uint32)
	// base information body length
	GetPayloadLength() int

	// base set body length
	SetPayloadLength(l int32)

	// use for router rpc
	GetCmd() string

	// base set cmd
	SetCmd(c string)

	// Print header information
	String() string

	SetTaskId(t string)
	GetTaskId() string
}

type Codec interface {

	// unpack header first
	// if err is not nil, return header
	GetHeader(b *bytes.Buffer) (Header, error)

	GetPayload(b *bytes.Buffer, len int) ([]byte, error)

	Unmarshal(b []byte, req reflect.Value) error

	Marshal(rsp reflect.Value) ([]byte, error)

	GetHeaderBytes(h Header) ([]byte, error)

	SetHeader(cmd string, payloadLength int32) Header
}
