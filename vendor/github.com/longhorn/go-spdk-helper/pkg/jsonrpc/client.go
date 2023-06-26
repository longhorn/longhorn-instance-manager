package jsonrpc

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	DefaultConcurrentLimit = 1024

	DefaultResponseReadWaitPeriod = 10 * time.Millisecond

	DefaultShortTimeout = 30 * time.Second
	DefaultLongTimeout  = 24 * time.Hour
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

		idCounter: rand.Uint32() % 10000,

		encoder: e,
		decoder: json.NewDecoder(bufio.NewReader(conn)),

		msgWrapperQueue:   make(chan *messageWrapper, DefaultConcurrentLimit),
		respReceiverQueue: make(chan map[string]interface{}, DefaultConcurrentLimit),
		responseChans:     make(map[uint32]chan []byte),
	}
}

func (c *Client) SendMsgWithTimeout(method string, params interface{}, timeout time.Duration) (res []byte, err error) {
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

	connEncoder := json.NewEncoder(c.conn)
	connEncoder.SetIndent("", "\t")
	if err = connEncoder.Encode(msg); err != nil {
		return nil, err
	}

	connDecoder := json.NewDecoder(bufio.NewReader(c.conn))
	for count := 0; count <= int(timeout/time.Second); count++ {
		if connDecoder.More() {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if !connDecoder.More() {
		return nil, fmt.Errorf("timeout %v second waiting for the response from the SPDK JSON RPC server", timeout.Seconds())
	}

	if err = connDecoder.Decode(&resp); err != nil {
		return nil, err
	}
	if resp.ID != id {
		return nil, fmt.Errorf("received response but the id mismatching, message id %d, response id %d", id, resp.ID)
	}

	if resp.ErrorInfo != nil {
		return nil, resp.ErrorInfo
	}

	buf := bytes.Buffer{}
	jsonEncoder := json.NewEncoder(&buf)
	if err := jsonEncoder.Encode(resp.Result); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *Client) SendCommand(method string, params interface{}) ([]byte, error) {
	return c.SendMsgWithTimeout(method, params, DefaultShortTimeout)
}

func (c *Client) SendCommandWithLongTimeout(method string, params interface{}) ([]byte, error) {
	return c.SendMsgWithTimeout(method, params, DefaultLongTimeout)
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
		logrus.WithError(err).Errorf("Failed to encode during handleSend")
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
		logrus.WithError(err).Errorf("Failed to encode received object during handleRecv, id: %T, result: %+v", obj["id"], obj["result"])
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
	ticker := time.NewTicker(DefaultResponseReadWaitPeriod)

	for {
		select {
		case <-ticker.C:
			if !decoder.More() {
				continue
			}

			var obj map[string]interface{}
			if err := decoder.Decode(&obj); err != nil {
				logrus.WithError(err).Errorf("Failed to decoding during read")
				errChan <- err
				continue
			}
			c.respReceiverQueue <- obj
		}
	}
}
