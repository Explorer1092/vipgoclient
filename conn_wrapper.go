// Copyright 2019 zhvala
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pulsar

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"code.vipkid.com.cn/zhanghao1/go-pulsar-client/auth"
	"code.vipkid.com.cn/zhanghao1/go-pulsar-client/frame"
	"code.vipkid.com.cn/zhanghao1/go-pulsar-client/pkg/api"
	"code.vipkid.com.cn/zhanghao1/go-pulsar-client/utils"
	"github.com/golang/protobuf/proto"
)

var (
	errConnectionClosed   = errors.New("connection closed")
	errSendingQueueClosed = errors.New("sending queue closed")
	errHandlerNotFound    = errors.New("handler not found")
)

type ProduceHandler interface {
	HandleConnected(frame.Frame) error
	HandleReceipt(frame.Frame) error
	HandleError(frame.Frame) error
	HandleClose(frame.Frame) error
}

type ConsumeHandler interface {
	HandleConnected(frame.Frame) error
	HandleMessage(frame.Frame) error
	HandleEndOfTopic(frame.Frame) error
	HandleClose(frame.Frame) error
}

// ConnWrapper is a wrapper of Conn which pack and send commands to pulsar server.
type ConnWrapper struct {
	LogicalAddr, PhysicalAddr string
	C                         *Conn
	RequestID                 *MonotonicID
	Dispatcher                *frame.Dispatcher
	ConsumeHandlers           map[uint64]ConsumeHandler
	ProduceHandlers           map[uint64]ProduceHandler
	rwmu                      sync.RWMutex
}

// Start handshake with pulsar server.
func (cw *ConnWrapper) Start(timeout time.Duration, authMethod auth.Authentication) error {
	go cw.recvLoop()
	ctx, cancelTimeout := context.WithTimeout(context.Background(), timeout)
	defer cancelTimeout()

	resp, cancelReg, err := cw.Dispatcher.RegisterGlobal()
	if err != nil {
		return err
	}
	defer cancelReg()

	// NOTE: The source see
	// RequestID will be -1 (ie UndefRequestID) in the case that it's
	// associated with a CONNECT request.
	// https://github.com/apache/incubator-pulsar/blob/fdc7b8426d8253c9437777ae51a4639239550f00/pulsar-broker/src/main/java/org/apache/pulsar/broker/service/ServerCnx.java#L325
	errResp, cancelReq, err := cw.Dispatcher.RegisterReqID(utils.UndefRequestID)
	if err != nil {
		return err
	}
	defer cancelReq()

	// create and send CONNECT msg
	connect := api.CommandConnect{
		ClientVersion:   proto.String(utils.ClientVersion),
		ProtocolVersion: proto.Int32(utils.ProtoVersion),
	}

	if authMethod != nil {
		connect.AuthMethodName = proto.String(authMethod.GetAuthName())
		connect.AuthData = []byte(authMethod.GetCommandData())
	}
	if cw.LogicalAddr != cw.PhysicalAddr {
		connect.ProxyToBrokerUrl = proto.String(cw.LogicalAddr)
	}

	frame := &frame.Frame{
		BaseCmd: &api.BaseCommand{
			Type:    api.BaseCommand_CONNECT.Enum(),
			Connect: &connect,
		},
	}

	if err := cw.C.Write(ctx, frame); err != nil {
		return err
	}

	// wait for the response, error, or timeout
	select {
	case <-cw.C.Closed():
		return errConnectionClosed
	case errFrame := <-errResp:
		errMsg := errFrame.BaseCmd.GetError()
		return fmt.Errorf("%s: %s", errMsg.GetError().String(), errMsg.GetMessage())
	case <-resp:
		return nil
	}
	// return errors.New("unexcepted handshake result")
}

func (cw *ConnWrapper) RegisterProducer(ctx context.Context, topic, producerName string, producerID uint64, handler ProduceHandler) error {
	requestID := cw.RequestID.Next()

	cmd := &api.BaseCommand{
		Type: api.BaseCommand_PRODUCER.Enum(),
		Producer: &api.CommandProducer{
			RequestId:  proto.Uint64(requestID),
			ProducerId: proto.Uint64(producerID),
			Topic:      proto.String(topic),
		},
	}
	if producerName != "" {
		cmd.Producer.ProducerName = proto.String(producerName)
	}

	frame := &frame.Frame{
		BaseCmd: cmd,
	}

	resp, cancel, err := cw.Dispatcher.RegisterReqID(requestID)
	if err != nil {
		return err
	}
	defer cancel()

	if err := cw.C.Write(ctx, frame); err != nil {
		return err
	}

	// wait for the success response, error, or timeout
	select {
	case <-cw.C.Closed():
		return errConnectionClosed
	case f := <-resp:
		msgType := f.BaseCmd.GetType()
		// Possible responses types are:
		//  - ProducerSuccess
		//  - Error
		switch msgType {
		case api.BaseCommand_PRODUCER_SUCCESS:
			cw.addProduceHandler(producerID, handler)
			return handler.HandleConnected(f)
		case api.BaseCommand_ERROR:
			errMsg := f.BaseCmd.GetError()
			return fmt.Errorf("%s: %s", errMsg.GetError().String(), errMsg.GetMessage())
		default:
			return utils.NewUnexpectedErrMsg(msgType, requestID)
		}
	}
}

func (cw *ConnWrapper) UnregisterProducer(ctx context.Context, producerID uint64) error {
	requestID := cw.RequestID.Next()

	frame := &frame.Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_CLOSE_PRODUCER.Enum(),
			CloseProducer: &api.CommandCloseProducer{
				RequestId:  proto.Uint64(requestID),
				ProducerId: proto.Uint64(producerID),
			},
		},
	}

	resp, cancel, err := cw.Dispatcher.RegisterReqID(requestID)
	if err != nil {
		return err
	}
	defer cancel()

	if err := cw.C.Write(ctx, frame); err != nil {
		return err
	}

	// wait for the success response, error, or timeout
	select {
	case <-cw.C.Closed():
		return errConnectionClosed
	case <-ctx.Done():
		return ctx.Err()
	case <-resp:
		cw.deleteProduceHandler(producerID)
		return nil
	}
}

func (cw *ConnWrapper) Subscribe(ctx context.Context, topic, subName, consumerName string, subType SubscriptionType,
	consumerID uint64, initialPosition InitialPosition, handler ConsumeHandler) error {
	requestID := cw.RequestID.Next()

	var sub api.CommandSubscribe_SubType
	switch subType {
	case Exclusive:
		sub = api.CommandSubscribe_Exclusive
	case Shared:
		sub = api.CommandSubscribe_Shared
	case Failover:
		sub = api.CommandSubscribe_Failover
	case KeyShare:
		sub = api.CommandSubscribe_Key_Shared
	}

	var position api.CommandSubscribe_InitialPosition
	switch initialPosition {
	case Latest:
		position = api.CommandSubscribe_Latest
	case Earliest:
		position = api.CommandSubscribe_Earliest
	}

	frame := &frame.Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_SUBSCRIBE.Enum(),
			Subscribe: &api.CommandSubscribe{
				SubType:      sub.Enum(),
				Topic:        proto.String(topic),
				Subscription: proto.String(subName),
				RequestId:    proto.Uint64(requestID),
				ConsumerId:   proto.Uint64(consumerID),
				ConsumerName: proto.String(consumerName),
				// hard code for now
				Durable:         proto.Bool(true),
				ReadCompacted:   proto.Bool(false),
				InitialPosition: position.Enum(),
			},
		},
	}

	resp, cancel, err := cw.Dispatcher.RegisterReqID(requestID)
	if err != nil {
		return err
	}
	defer cancel()

	if err := cw.C.Write(ctx, frame); err != nil {
		return err
	}

	// wait for the success response, error, or timeout
	select {
	case <-cw.C.Closed():
		return errConnectionClosed
	case <-ctx.Done():
		return ctx.Err()
	case f := <-resp:
		msgType := f.BaseCmd.GetType()
		// Possible responses types are:
		//  - Success (why not SubscribeSuccess?)
		//  - Error
		switch msgType {
		case api.BaseCommand_SUCCESS:
			cw.addConsumeHandler(consumerID, handler)
			return handler.HandleConnected(f)
		case api.BaseCommand_ERROR:
			errMsg := f.BaseCmd.GetError()
			return fmt.Errorf("%s: %s", errMsg.GetError().String(), errMsg.GetMessage())
		default:
			return utils.NewUnexpectedErrMsg(msgType, requestID)
		}
	}
}

func (cw *ConnWrapper) Unubscribe(ctx context.Context, consumerID uint64) error {
	requestID := cw.RequestID.Next()

	frame := &frame.Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_UNSUBSCRIBE.Enum(),
			Unsubscribe: &api.CommandUnsubscribe{
				RequestId:  proto.Uint64(requestID),
				ConsumerId: proto.Uint64(consumerID),
			},
		},
	}

	resp, cancel, err := cw.Dispatcher.RegisterReqID(requestID)
	if err != nil {
		return err
	}
	defer cancel()

	if err := cw.C.Write(ctx, frame); err != nil {
		return err
	}

	select {
	case <-cw.C.Closed():
		return errConnectionClosed
	case <-ctx.Done():
		return ctx.Err()
	case <-resp:
		cw.deleteConsumeHandler(consumerID)
		return nil
	}
}

func (cw *ConnWrapper) CloseConsumer(ctx context.Context, consumerID uint64) error {
	requestID := cw.RequestID.Next()

	frame := &frame.Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_CLOSE_CONSUMER.Enum(),
			CloseConsumer: &api.CommandCloseConsumer{
				RequestId:  proto.Uint64(requestID),
				ConsumerId: proto.Uint64(consumerID),
			},
		},
	}

	resp, cancel, err := cw.Dispatcher.RegisterReqID(requestID)
	if err != nil {
		return err
	}
	defer cancel()

	if err := cw.C.Write(ctx, frame); err != nil {
		return err
	}

	select {
	case <-cw.C.Closed():
		return errConnectionClosed
	case <-ctx.Done():
		return ctx.Err()

	case <-resp:
		cw.deleteConsumeHandler(consumerID)
		return nil
	}
}

func (cw *ConnWrapper) Ack(ctx context.Context, consumerID uint64, msgID MessageID) error {
	id := &api.MessageIdData{
		LedgerId:   proto.Uint64(msgID.LedgerId),
		EntryId:    proto.Uint64(msgID.EntryId),
		Partition:  proto.Int32(msgID.Partition),
		BatchIndex: proto.Int32(msgID.BatchIndex),
	}
	frame := &frame.Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_ACK.Enum(),
			Ack: &api.CommandAck{
				ConsumerId: proto.Uint64(consumerID),
				MessageId:  []*api.MessageIdData{id},
				AckType:    api.CommandAck_Individual.Enum(),
			},
		},
	}

	return cw.C.Write(ctx, frame)
}

func (cw *ConnWrapper) Flow(ctx context.Context, consumerID uint64, permits uint32) error {
	if permits <= 0 {
		return fmt.Errorf("invalid number of permits requested: %d", permits)
	}

	frame := &frame.Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_FLOW.Enum(),
			Flow: &api.CommandFlow{
				ConsumerId:     proto.Uint64(consumerID),
				MessagePermits: proto.Uint32(permits),
			},
		},
	}

	return cw.C.Write(ctx, frame)
}

func (cw *ConnWrapper) Seek(ctx context.Context, consumerID uint64, msgID MessageID) error {
	requestID := cw.RequestID.Next()
	id := &api.MessageIdData{
		LedgerId:   proto.Uint64(msgID.LedgerId),
		EntryId:    proto.Uint64(msgID.EntryId),
		Partition:  proto.Int32(msgID.Partition),
		BatchIndex: proto.Int32(msgID.BatchIndex),
	}
	frame := &frame.Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_SEEK.Enum(),
			Seek: &api.CommandSeek{
				ConsumerId: proto.Uint64(consumerID),
				RequestId:  proto.Uint64(requestID),
				MessageId:  id,
			},
		},
	}

	resp, cancel, err := cw.Dispatcher.RegisterReqID(requestID)
	if err != nil {
		return err
	}
	defer cancel()

	if err := cw.C.Write(ctx, frame); err != nil {
		return err
	}

	select {
	case <-cw.C.Closed():
		return errConnectionClosed
	case <-ctx.Done():
		return ctx.Err()

	case <-resp:
		return nil
	}
}

func (cw *ConnWrapper) RedeliverUnacked(ctx context.Context, consumerID uint64) error {
	frame := &frame.Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_REDELIVER_UNACKNOWLEDGED_MESSAGES.Enum(),
			RedeliverUnacknowledgedMessages: &api.CommandRedeliverUnacknowledgedMessages{
				ConsumerId: proto.Uint64(consumerID),
			},
		},
	}

	return cw.C.Write(ctx, frame)
}

func (cw *ConnWrapper) TopicsOfNamespace(ctx context.Context, namespace string) (*api.CommandGetTopicsOfNamespaceResponse, error) {
	requestID := cw.RequestID.Next()
	frame := &frame.Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_GET_TOPICS_OF_NAMESPACE.Enum(),
			GetTopicsOfNamespace: &api.CommandGetTopicsOfNamespace{
				RequestId: proto.Uint64(requestID),
				Namespace: proto.String(namespace),
			},
		},
	}

	resp, cancel, err := cw.Dispatcher.RegisterReqID(requestID)
	if err != nil {
		return nil, err
	}
	defer cancel()

	if err := cw.C.Write(ctx, frame); err != nil {
		return nil, err
	}

	// wait for response or timeout
	select {
	case <-cw.C.Closed():
		return nil, errConnectionClosed
	case <-ctx.Done():
		return nil, ctx.Err()

	case f := <-resp:
		return f.BaseCmd.GetGetTopicsOfNamespaceResponse(), nil
	}
}

func (cw *ConnWrapper) PartitionedMetadata(ctx context.Context, topic string) (*api.CommandPartitionedTopicMetadataResponse, error) {
	requestID := cw.RequestID.Next()
	frame := &frame.Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_PARTITIONED_METADATA.Enum(),
			PartitionMetadata: &api.CommandPartitionedTopicMetadata{
				RequestId: proto.Uint64(requestID),
				Topic:     proto.String(topic),
			},
		},
	}

	resp, cancel, err := cw.Dispatcher.RegisterReqID(requestID)
	if err != nil {
		return nil, err
	}
	defer cancel()

	if err := cw.C.Write(ctx, frame); err != nil {
		return nil, err
	}

	// wait for response or timeout
	select {
	case <-cw.C.Closed():
		return nil, errConnectionClosed
	case <-ctx.Done():
		return nil, ctx.Err()

	case f := <-resp:
		return f.BaseCmd.GetPartitionMetadataResponse(), nil
	}
}

func (cw *ConnWrapper) LookupTopic(ctx context.Context, topic string, authoritative bool) (*api.CommandLookupTopicResponse, error) {
	requestID := cw.RequestID.Next()

	frame := &frame.Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_LOOKUP.Enum(),
			LookupTopic: &api.CommandLookupTopic{
				RequestId:     proto.Uint64(requestID),
				Topic:         proto.String(topic),
				Authoritative: proto.Bool(authoritative),
			},
		},
	}

	resp, cancel, err := cw.Dispatcher.RegisterReqID(requestID)
	if err != nil {
		return nil, err
	}
	defer cancel()

	if err := cw.C.Write(ctx, frame); err != nil {
		return nil, err
	}

	// wait for async response or timeout

	select {
	case <-cw.C.Closed():
		return nil, errConnectionClosed
	case <-ctx.Done():
		return nil, ctx.Err()

	case f := <-resp:
		return f.BaseCmd.GetLookupTopicResponse(), nil
	}
}

func (cw *ConnWrapper) Ping(ctx context.Context) error {
	resp, cancel, err := cw.Dispatcher.RegisterGlobal()
	if err != nil {
		return err
	}
	defer cancel()

	frame := &frame.Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_PING.Enum(),
			Ping: &api.CommandPing{},
		},
	}

	if err := cw.C.Write(ctx, frame); err != nil {
		return err
	}

	select {
	case <-cw.C.Closed():
		return errConnectionClosed
	case <-ctx.Done():
		return ctx.Err()
	case <-resp:
		// PONG received
	}
	return nil
}

func (cw *ConnWrapper) Publish(ctx context.Context, f *frame.Frame) error {
	return cw.C.Write(ctx, f)
}

func (cw *ConnWrapper) recvLoop() {
	defer func() {
		if err := cw.C.Close(); err != nil {
			log.Println(err)
		}
	}()

	if err := cw.C.Read(cw.handleFrame); err != nil {
		log.Println(err)
	}
}

func (cw *ConnWrapper) dispatch(f frame.Frame) {
	var err error

	msgType := f.BaseCmd.GetType()

	switch msgType {

	// Solicited responses with NO response ID associated

	case api.BaseCommand_CONNECTED:
		err = cw.Dispatcher.NotifyGlobal(f)

	case api.BaseCommand_PONG:
		err = cw.Dispatcher.NotifyGlobal(f)

	// Solicited responses with a requestID to correlate
	// it to its request

	case api.BaseCommand_SUCCESS:
		err = cw.Dispatcher.NotifyReqID(f.BaseCmd.GetSuccess().GetRequestId(), f)

	case api.BaseCommand_ERROR:
		err = cw.Dispatcher.NotifyReqID(f.BaseCmd.GetError().GetRequestId(), f)

	case api.BaseCommand_LOOKUP_RESPONSE:
		err = cw.Dispatcher.NotifyReqID(f.BaseCmd.GetLookupTopicResponse().GetRequestId(), f)

	case api.BaseCommand_PARTITIONED_METADATA_RESPONSE:
		err = cw.Dispatcher.NotifyReqID(f.BaseCmd.GetPartitionMetadataResponse().GetRequestId(), f)

	case api.BaseCommand_PRODUCER_SUCCESS:
		err = cw.Dispatcher.NotifyReqID(f.BaseCmd.GetProducerSuccess().GetRequestId(), f)

	// Solicited responses with a (producerID, sequenceID) tuple to correlate
	// it to its request

	case api.BaseCommand_SEND_RECEIPT:
		err = cw.handleReceipt(f.BaseCmd.GetSendReceipt().GetProducerId(), f)

	case api.BaseCommand_SEND_ERROR:
		err = cw.handleSendError(f.BaseCmd.GetSendReceipt().GetProducerId(), f)
		// msg := f.BaseCmd.GetSendError()
		// err = cw.Dispatcher.NotifyProdSeqIDs(msg.GetProducerId(), msg.GetSequenceId(), f)

	// Unsolicited responses that have a producer ID

	case api.BaseCommand_CLOSE_PRODUCER:
		err = cw.handleProducerClose(f.BaseCmd.GetCloseProducer().GetProducerId(), f)

	// Unsolicited responses that have a consumer ID

	case api.BaseCommand_CLOSE_CONSUMER:
		err = cw.handleConsumerClose(f.BaseCmd.GetCloseConsumer().GetConsumerId(), f)

	case api.BaseCommand_REACHED_END_OF_TOPIC:
		err = cw.handleEndOfTopic(f.BaseCmd.GetReachedEndOfTopic().GetConsumerId(), f)

	case api.BaseCommand_MESSAGE:
		err = cw.handleMessage(f.BaseCmd.GetMessage().GetConsumerId(), f)

	// Unsolicited responses

	case api.BaseCommand_PING:
		err = cw.handlePing(msgType, f.BaseCmd.GetPing())
	case api.BaseCommand_ACTIVE_CONSUMER_CHANGE:
		// f.BaseCmd.GetActiveConsumerChange()
		// ignore

	default:
		err = fmt.Errorf("unhandled message of type %q", f.BaseCmd.GetType())
	}

	if err != nil {
		log.Println(err)
	}
}

func (cw *ConnWrapper) addConsumeHandler(id uint64, handler ConsumeHandler) {
	cw.rwmu.Lock()
	cw.ConsumeHandlers[id] = handler
	cw.rwmu.Unlock()
}

func (cw *ConnWrapper) deleteConsumeHandler(id uint64) {
	cw.rwmu.Lock()
	delete(cw.ConsumeHandlers, id)
	cw.rwmu.Unlock()
}

func (cw *ConnWrapper) addProduceHandler(id uint64, handler ProduceHandler) {
	cw.rwmu.Lock()
	cw.ProduceHandlers[id] = handler
	cw.rwmu.Unlock()
}

func (cw *ConnWrapper) deleteProduceHandler(id uint64) {
	cw.rwmu.Lock()
	delete(cw.ProduceHandlers, id)
	cw.rwmu.Unlock()
}

func (cw *ConnWrapper) handleFrame(f frame.Frame) {
	// run in a new goroutine
	go cw.dispatch(f)
}

func (cw *ConnWrapper) handleReceipt(id uint64, f frame.Frame) error {
	cw.rwmu.RLock()
	handler, ok := cw.ProduceHandlers[id]
	cw.rwmu.RUnlock()
	if ok {
		return handler.HandleReceipt(f)
	}
	return errHandlerNotFound
}

func (cw *ConnWrapper) handleSendError(id uint64, f frame.Frame) error {
	cw.rwmu.RLock()
	handler, ok := cw.ProduceHandlers[id]
	cw.rwmu.RUnlock()
	if ok {
		return handler.HandleError(f)
	}
	return errHandlerNotFound
}

func (cw *ConnWrapper) handleProducerClose(id uint64, f frame.Frame) error {
	cw.rwmu.RLock()
	handler, ok := cw.ProduceHandlers[id]
	cw.rwmu.RUnlock()
	if ok {
		return handler.HandleClose(f)
	}
	return errHandlerNotFound
}

func (cw *ConnWrapper) handleMessage(id uint64, f frame.Frame) error {
	cw.rwmu.RLock()
	handler, ok := cw.ConsumeHandlers[id]
	cw.rwmu.RUnlock()
	if ok {
		return handler.HandleMessage(f)
	}
	return errHandlerNotFound
}

func (cw *ConnWrapper) handleEndOfTopic(id uint64, f frame.Frame) error {
	cw.rwmu.RLock()
	handler, ok := cw.ConsumeHandlers[id]
	cw.rwmu.RUnlock()
	if ok {
		return handler.HandleEndOfTopic(f)
	}
	return errHandlerNotFound
}

func (cw *ConnWrapper) handleConsumerClose(id uint64, f frame.Frame) error {
	cw.rwmu.RLock()
	handler, ok := cw.ConsumeHandlers[id]
	cw.rwmu.RUnlock()
	if ok {
		return handler.HandleClose(f)
	}
	return errHandlerNotFound
}

func (cw *ConnWrapper) handlePing(msgType api.BaseCommand_Type, msg *api.CommandPing) error {
	frame := &frame.Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_PONG.Enum(),
			Pong: &api.CommandPong{},
		},
	}

	return cw.C.Write(context.Background(), frame)
}

func (cw *ConnWrapper) Close() error {
	return cw.C.Close()
}

func (cw *ConnWrapper) IsClose() bool {
	return cw.C.IsClosed
}
