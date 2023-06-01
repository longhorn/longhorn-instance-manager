package jsonrpc

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	DefaultTimeoutInSecond = 30
)

type Client struct {
	conn net.Conn

	idCounter uint32

	encoder *json.Encoder
	decoder *json.Decoder

	// For async command only
	msgWrapperQueue   chan *messageWrapper
	respReceiverQueue chan map[string]interface{}
	responseChans     map[uint32]chan []byte
}

type messageWrapper struct {
	method   string
	params   interface{}
	response chan []byte
}

func NewClient(conn net.Conn) *Client {
	e := json.NewEncoder(conn)
	e.SetIndent("", "\t")

	return &Client{
		conn: conn,

		// Get lower 5 digits of the current process ID as the prefix of the message ID
		idCounter: uint32((os.Getpid() % (1 << 6)) * 10000),

		encoder: e,
		decoder: json.NewDecoder(bufio.NewReader(conn)),

		msgWrapperQueue:   make(chan *messageWrapper, 1024),
		respReceiverQueue: make(chan map[string]interface{}, 1024),
		responseChans:     make(map[uint32]chan []byte),
	}
}

func (c *Client) SendMsgWithTimeout(method string, params interface{}, timeoutInSec int) (res []byte, err error) {
	id := atomic.AddUint32(&c.idCounter, 1)
	msg := NewMessage(id, method, params)
	var resp Response

	defer func() {
		if err != nil {
			err = JSONClientError{
				ID:          id,
				Method:      method,
				Params:      params,
				ErrorDetail: err,
			}
		}

		// For debug purpose
		stdenc := json.NewEncoder(os.Stdin)
		stdenc.SetIndent("", "\t")
		stdenc.Encode(msg)
		stdenc.Encode(&resp)
	}()

	marshaledParams, err := json.Marshal(msg.Params)
	if err != nil {
		return nil, err
	}
	if string(marshaledParams) == "{}" {
		msg.Params = nil
	}

	if err = c.encoder.Encode(msg); err != nil {
		return nil, err
	}

	for count := 0; count <= timeoutInSec; count++ {
		if c.decoder.More() {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if err = c.decoder.Decode(&resp); err != nil {
		return nil, err
	}
	if resp.ID != id {
		return nil, fmt.Errorf("received response but the id mismatching, message id %d, response id %d", id, resp.ID)
	}

	if resp.ErrorInfo != nil {
		return nil, resp.ErrorInfo
	}

	buf := bytes.Buffer{}
	e := json.NewEncoder(&buf)
	if err := e.Encode(resp.Result); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *Client) SendMsg(method string, params interface{}) ([]byte, error) {
	return c.SendMsgWithTimeout(method, params, DefaultTimeoutInSecond)
}

func (c *Client) SendCommand(method string, params interface{}) ([]byte, error) {
	return c.SendMsg(method, params)
}

func (c *Client) InitAsync() chan error {
	errChan := make(chan error, 5)
	go c.dispatcher()
	go c.read(errChan)
	return errChan
}

func (c *Client) SendMsgAsync(method string, params interface{}, responseChan chan []byte) {
	msgWrapper := &messageWrapper{method: method,
		params:   params,
		response: responseChan}

	c.msgWrapperQueue <- msgWrapper
}

func (c *Client) handleSend(msgWrapper *messageWrapper) {
	id := atomic.AddUint32(&c.idCounter, 1)
	c.responseChans[id] = msgWrapper.response

	if err := c.encoder.Encode(NewMessage(id, msgWrapper.method, msgWrapper.params)); err != nil {
		logrus.Errorf("error encoding during handleSend: %v", err)
	}
}

func (c *Client) handleRecv(obj map[string]interface{}) {
	fid, ok := obj["id"].(float64)
	if ok {
		logrus.Errorf("Invalid received object during handleRecv: %T", obj["id"])
		return
	}

	id := uint32(fid)
	ch := c.responseChans[id]
	delete(c.responseChans, id)

	// TODO: Not sure if this would lead to heavy GC
	buf := bytes.Buffer{}
	e := json.NewEncoder(&buf)
	if err := e.Encode(obj["result"]); err != nil {
		logrus.Errorf("Failed to encode received object during handleRecv, id: %T, result: %+v", obj["id"], obj["result"])
		return
	}
	ch <- buf.Bytes()
}

func (c *Client) dispatcher() {
	for {
		select {
		case msg := <-c.msgWrapperQueue:
			c.handleSend(msg)
		case resp := <-c.respReceiverQueue:
			c.handleRecv(resp)
		}
	}
}

func (c *Client) read(errChan chan error) {
	decoder := json.NewDecoder(c.conn)

	for decoder.More() {
		var obj map[string]interface{}

		if err := decoder.Decode(&obj); err != nil {
			logrus.Errorf("error decoding during read: %v", err)
			errChan <- err
			continue
		}

		c.respReceiverQueue <- obj
	}
}
