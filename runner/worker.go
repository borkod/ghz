package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// TickValue is the tick value
type TickValue struct {
	instant   time.Time
	reqNumber uint64
}

// BidiStream represents a bi-directional stream for sending messages to and receiving
// messages from a server. The header and trailer metadata sent by the server can also be
// queried.
type BidiStream struct {
	stream   grpc.ClientStream
	reqType  protoreflect.MessageDescriptor
	respType protoreflect.MessageDescriptor
	//mf       *dynamic.MessageFactory
}

// Worker is used for doing a single stream of requests in parallel
type Worker struct {
	stub Channel
	mtd  protoreflect.MethodDescriptor

	config   *RunConfig
	workerID string
	active   bool
	stopCh   chan bool
	ticks    <-chan TickValue

	dataProvider     DataProviderFunc
	metadataProvider MetadataProviderFunc
	msgProvider      StreamMessageProviderFunc

	streamRecv                    StreamRecvMsgInterceptFunc
	streamInterceptorProviderFunc StreamInterceptorProviderFunc
}

func (w *Worker) runWorker() error {
	var err error
	g := new(errgroup.Group)

	for {
		select {
		case <-w.stopCh:
			if w.config.async {
				return g.Wait()
			}

			return err
		case tv := <-w.ticks:
			if w.config.async {
				g.Go(func() error {
					return w.makeRequest(tv)
				})
			} else {
				rErr := w.makeRequest(tv)
				err = multierr.Append(err, rErr)
			}
		}
	}
}

// Stop stops the worker. It has to be started with Run() again.
func (w *Worker) Stop() {
	if !w.active {
		return
	}

	w.active = false
	w.stopCh <- true
}

func (w *Worker) makeRequest(tv TickValue) error {
	reqNum := int64(tv.reqNumber)

	ctd := newCallData(w.mtd, w.workerID, reqNum, !w.config.disableTemplateFuncs, !w.config.disableTemplateData, w.config.funcs)

	var streamInterceptor StreamInterceptor
	if w.mtd.IsStreamingClient() || w.mtd.IsStreamingServer() {
		if w.streamInterceptorProviderFunc != nil {
			streamInterceptor = w.streamInterceptorProviderFunc(ctd)
		}
	}

	reqMD, err := w.metadataProvider(ctd)
	if err != nil {
		return err
	}

	if w.config.enableCompression {
		reqMD.Set("grpc-accept-encoding", gzip.Name)
	}

	ctx := context.Background()
	var cancel context.CancelFunc

	if w.config.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, w.config.timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	// include the metadata
	if reqMD != nil {
		ctx = metadata.NewOutgoingContext(ctx, *reqMD)
	}

	inputs, err := w.dataProvider(ctd)
	if err != nil {
		return err
	}

	var msgProvider StreamMessageProviderFunc
	if w.msgProvider != nil {
		msgProvider = w.msgProvider
	} else if streamInterceptor != nil {
		msgProvider = streamInterceptor.Send
	} else if w.mtd.IsStreamingClient() {
		if w.config.streamDynamicMessages {
			mp, err := newDynamicMessageProvider(w.mtd, w.config.data, w.config.streamCallCount, !w.config.disableTemplateFuncs, !w.config.disableTemplateData)
			if err != nil {
				return err
			}

			msgProvider = mp.GetStreamMessage
		} else {
			mp, err := newStaticMessageProvider(w.config.streamCallCount, inputs)
			if err != nil {
				return err
			}

			msgProvider = mp.GetStreamMessage
		}
	}

	if len(inputs) == 0 && msgProvider == nil {
		return fmt.Errorf("no data provided for request")
	}

	var callType string
	if w.config.hasLog {
		callType = "unary"
		if w.mtd.IsStreamingClient() && w.mtd.IsStreamingServer() {
			callType = "bidi"
		} else if w.mtd.IsStreamingServer() {
			callType = "server-streaming"
		} else if w.mtd.IsStreamingClient() {
			callType = "client-streaming"
		}

		w.config.log.Debugw("Making request", "workerID", w.workerID,
			"call type", callType, "call", w.mtd.FullName(),
			"input", inputs, "metadata", reqMD)
	}

	// RPC errors are handled via stats handler
	if w.mtd.IsStreamingClient() && w.mtd.IsStreamingServer() {
		_ = w.makeBidiRequest(&ctx, ctd, msgProvider, streamInterceptor)
	} else if w.mtd.IsStreamingClient() {
		_ = w.makeClientStreamingRequest(&ctx, ctd, msgProvider)
	} else if w.mtd.IsStreamingServer() {
		_ = w.makeServerStreamingRequest(&ctx, inputs[0], streamInterceptor)
	} else {
		_ = w.makeUnaryRequest(&ctx, reqMD, inputs[0])
	}

	return err
}

func requestMethod(md protoreflect.MethodDescriptor) string {
	return fmt.Sprintf("/%s/%s", md.Parent().FullName(), md.Name())
}

func methodType(md protoreflect.MethodDescriptor) string {
	if md.IsStreamingClient() && md.IsStreamingServer() {
		return "bidi-streaming"
	} else if md.IsStreamingClient() {
		return "client-streaming"
	} else if md.IsStreamingServer() {
		return "server-streaming"
	} else {
		return "unary"
	}
}

func checkMessageType(md protoreflect.MessageDescriptor, dm *dynamicpb.Message) error {
	typeName := string(dm.Descriptor().FullName())
	if typeName != string(md.FullName()) {
		return fmt.Errorf("expecting message of type %s; got %s", md.FullName(), typeName)
	}
	return nil
}

func (w *Worker) makeUnaryRequest(ctx *context.Context, reqMD *metadata.MD, input *dynamicpb.Message) error {
	if w.mtd.IsStreamingClient() || w.mtd.IsStreamingServer() {
		// TODO: perform w.config.log.debugw
		return fmt.Errorf("InvokeRpc is for unary methods; %q is %s", w.mtd.FullName(), methodType(w.mtd))
	}
	if err := checkMessageType(w.mtd.Input(), input); err != nil {
		// TODO: perform w.config.log.debugw
		return err
	}

	var callOptions = []grpc.CallOption{}
	if w.config.enableCompression {
		callOptions = append(callOptions, grpc.UseCompressor(gzip.Name))
	}

	res := dynamicpb.NewMessage(w.mtd.Output())
	resErr := w.stub.Invoke(*ctx, requestMethod(w.mtd), input, res, callOptions...)

	if w.config.hasLog {
		inputData, _ := protojson.Marshal(input)
		resData, _ := protojson.Marshal(res)

		w.config.log.Debugw("Received response", "workerID", w.workerID, "call type", "unary",
			"call", w.mtd.FullName(),
			"input", string(inputData), "metadata", reqMD,
			"response", string(resData), "error", resErr)
	}

	return resErr
}

func (w *Worker) makeClientStreamingRequest(ctx *context.Context,
	ctd *CallData, messageProvider StreamMessageProviderFunc) error {
	var str grpc.ClientStream
	var err error
	var callOptions = []grpc.CallOption{}
	if w.config.enableCompression {
		callOptions = append(callOptions, grpc.UseCompressor(gzip.Name))
	}

	if !w.mtd.IsStreamingClient() || w.mtd.IsStreamingServer() {
		// TODO: perform w.config.log.debugw
		return fmt.Errorf("InvokeRpcClientStream is for client-streaming methods; %q is %s", w.mtd.FullName(), methodType(w.mtd))
	}
	ctxCancel, callCancel := context.WithCancel(*ctx)
	sd := grpc.StreamDesc{
		StreamName:    string(w.mtd.FullName()),
		ServerStreams: w.mtd.IsStreamingServer(),
		ClientStreams: w.mtd.IsStreamingClient(),
	}
	if str, err = w.stub.NewStream(ctxCancel, &sd, requestMethod(w.mtd), callOptions...); err != nil {
		callCancel()
		if w.config.hasLog {
			w.config.log.Errorw("Invoke Client Streaming RPC call error: "+err.Error(), "workerID", w.workerID,
				"call type", "client-streaming",
				"call", w.mtd.FullName(), "error", err)
		}
		return err
	} else {
		go func() {
			// when the new stream is finished, also cleanup the parent context
			<-str.Context().Done()
			callCancel()
		}()
	}

	// TODO: should this return error? right now we just log errors?
	closeStream := func() {
		if closeErr := str.CloseSend(); err != nil {
			w.config.log.Debugw("Close send", "workerID", w.workerID, "call type", "client-streaming",
				"call", w.mtd.FullName(),
				"error", closeErr)
		}
		resp := dynamicpb.NewMessage(w.mtd.Output())
		if err := str.RecvMsg(resp); err != nil {
			if w.config.hasLog {
				w.config.log.Debugw("Close and receive", "workerID", w.workerID, "call type", "client-streaming",
					"call", w.mtd.FullName(),
					"response", resp, "error", err)
			}
		}
		if err := str.RecvMsg(resp); err != io.EOF {
			if err == nil {
				callCancel()
				// TODO: do proper debug log
				w.config.log.Debugw("client-streaming method %q returned more than one response message", w.mtd.FullName())
			} else {
				// TODO: do proper debug log
				w.config.log.Debugw("client-streaming method %q returned more than one response message", w.mtd.FullName())
			}
		}

		//res, closeErr := str.CloseAndReceive()

		// if w.config.hasLog {
		// 	w.config.log.Debugw("Close and receive", "workerID", w.workerID, "call type", "client-streaming",
		// 		"call", w.mtd.FullName(),
		// 		"response", res, "error", closeErr)
		// }
	}

	performSend := func(payload *dynamicpb.Message) (bool, error) {
		if err := checkMessageType(w.mtd.Input(), payload); err != nil {
			// TODO: do proper debug log
			w.config.log.Debugw("Send message", "workerID", w.workerID, "call type", "client-streaming")
			return false, err
		}

		err := str.SendMsg(payload)

		// TODO: WHY is it not checking for err != nil here?
		if w.config.hasLog {
			w.config.log.Debugw("Send message", "workerID", w.workerID, "call type", "client-streaming",
				"call", w.mtd.FullName(),
				"payload", payload, "error", err)
		}

		if err == io.EOF {
			return true, nil
		}

		return false, err
	}

	doneCh := make(chan struct{})
	cancel := make(chan struct{}, 1)
	if w.config.streamCallDuration > 0 {
		go func() {
			sct := time.NewTimer(w.config.streamCallDuration)
			select {
			case <-sct.C:
				cancel <- struct{}{}
				return
			case <-doneCh:
				if !sct.Stop() {
					<-sct.C
				}
				return
			}
		}()
	}

	done := false
	counter := uint(0)
	end := false
	for !done && len(cancel) == 0 {
		// default message provider checks counter
		// but we also need to keep our own counts
		// in case of custom client providers

		var payload *dynamicpb.Message
		payload, err = messageProvider(ctd)

		isLast := false
		if errors.Is(err, ErrLastMessage) {
			isLast = true
			err = nil
		}

		if err != nil {
			if errors.Is(err, ErrEndStream) {
				err = nil
			}
			break
		}

		end, err = performSend(payload)
		if end || err != nil || isLast || len(cancel) > 0 {
			break
		}

		counter++

		if w.config.streamCallCount > 0 && counter >= w.config.streamCallCount {
			break
		}

		if w.config.streamInterval > 0 {
			wait := time.NewTimer(w.config.streamInterval)
			select {
			case <-wait.C:
				break
			case <-cancel:
				if !wait.Stop() {
					<-wait.C
				}
				done = true
				break
			}
		}
	}

	for len(cancel) > 0 {
		<-cancel
	}

	closeStream()

	close(doneCh)
	close(cancel)

	return nil
}

func (w *Worker) makeServerStreamingRequest(ctx *context.Context, input *dynamicpb.Message, streamInterceptor StreamInterceptor) error {
	if w.mtd.IsStreamingClient() || !w.mtd.IsStreamingServer() {
		return fmt.Errorf("InvokeRpcServerStream is for server-streaming methods; %q is %s", w.mtd.FullName(), methodType(w.mtd))
	}
	if err := checkMessageType(w.mtd.Input(), input); err != nil {
		return err
	}

	var str grpc.ClientStream
	var err error
	var callOptions = []grpc.CallOption{}
	if w.config.enableCompression {
		callOptions = append(callOptions, grpc.UseCompressor(gzip.Name))
	}

	callCtx, callCancel := context.WithCancel(*ctx)
	defer callCancel()

	respType := w.mtd.Output()
	sd := grpc.StreamDesc{
		StreamName:    string(w.mtd.Name()),
		ServerStreams: w.mtd.IsStreamingServer(),
		ClientStreams: w.mtd.IsStreamingClient(),
	}
	if str, err = w.stub.NewStream(callCtx, &sd, requestMethod(w.mtd), callOptions...); err != nil {
		callCancel()
		if w.config.hasLog {
			w.config.log.Errorw("Invoke Server Streaming RPC call error: "+err.Error(), "workerID", w.workerID,
				"call type", "server-streaming",
				"call", w.mtd.FullName(),
				"input", input, "error", err)
		}
		return err
	} else {
		err = str.SendMsg(input)
		if err != nil {
			callCancel()
			if w.config.hasLog {
				w.config.log.Errorw("Invoke Server Streaming RPC call error: "+err.Error(), "workerID", w.workerID,
					"call type", "server-streaming",
					"call", w.mtd.FullName(),
					"input", input, "error", err)
			}
			return err
		}
		err = str.CloseSend()
		if err != nil {
			callCancel()
			if w.config.hasLog {
				w.config.log.Errorw("Invoke Server Streaming RPC call error: "+err.Error(), "workerID", w.workerID,
					"call type", "server-streaming",
					"call", w.mtd.FullName(),
					"input", input, "error", err)
			}
			return err
		}
		go func() {
			// when the new stream is finished, also cleanup the parent context
			<-str.Context().Done()
			callCancel()
		}()
	}

	// if err != nil {
	// 	if w.config.hasLog {
	// 		w.config.log.Errorw("Invoke Server Streaming RPC call error: "+err.Error(), "workerID", w.workerID,
	// 			"call type", "server-streaming",
	// 			"call", w.mtd.FullName(),
	// 			"input", input, "error", err)
	// 	}

	// 	return err
	// }

	doneCh := make(chan struct{})
	cancel := make(chan struct{}, 1)
	if w.config.streamCallDuration > 0 {
		go func() {
			sct := time.NewTimer(w.config.streamCallDuration)
			select {
			case <-sct.C:
				cancel <- struct{}{}
				return
			case <-doneCh:
				if !sct.Stop() {
					<-sct.C
				}
				return
			}
		}()
	}

	interceptCanceled := false
	counter := uint(0)
	for err == nil {
		// we should check before receiving a message too
		if w.config.streamCallDuration > 0 && len(cancel) > 0 {
			<-cancel
			callCancel()
			break
		}

		resp := dynamicpb.NewMessage(respType)
		if err := str.RecvMsg(resp); err != nil {
			if w.config.hasLog {
				w.config.log.Debugw("Receive message", "workerID", w.workerID, "call type", "server-streaming",
					"call", w.mtd.FullName(),
					"response", resp, "error", err)
			}
		}

		// with any of the cancellation operations we can't just bail
		// we have to drain the messages until the server gets the cancel and ends their side of the stream

		if w.streamRecv != nil {
			//if converted, ok := res.(*dynamicpb.Message); ok {
			err = w.streamRecv(resp, err)
			if errors.Is(err, ErrEndStream) && !interceptCanceled {
				interceptCanceled = true
				err = nil

				callCancel()
			}
			//}
		}

		if streamInterceptor != nil {
			//if converted, ok := res.(*dynamicpb.Message); ok {
			err = streamInterceptor.Recv(resp, err)
			if errors.Is(err, ErrEndStream) && !interceptCanceled {
				interceptCanceled = true
				err = nil

				callCancel()
			}
			//}
		}

		if err != nil {
			if err == io.EOF {
				err = nil
			}

			break
		}

		counter++

		if w.config.streamCallCount > 0 && counter >= w.config.streamCallCount {
			callCancel()
		}

		if w.config.streamCallDuration > 0 && len(cancel) > 0 {
			<-cancel
			callCancel()
		}
	}

	close(doneCh)
	close(cancel)

	return err
}

func (w *Worker) makeBidiRequest(ctx *context.Context,
	ctd *CallData, messageProvider StreamMessageProviderFunc, streamInterceptor StreamInterceptor) error {

	var callOptions = []grpc.CallOption{}

	if w.config.enableCompression {
		callOptions = append(callOptions, grpc.UseCompressor(gzip.Name))
	}

	var err error
	var str *BidiStream
	//str, err := w.stub.InvokeRpcBidiStream(*ctx, w.mtd, callOptions...)
	if !w.mtd.IsStreamingClient() || !w.mtd.IsStreamingServer() {
		if w.config.hasLog {
			w.config.log.Errorw("makeBidiRequest is for bidi-streaming methods; %q is %s", w.mtd.FullName(), methodType(w.mtd),
				"workerID", w.workerID, "call type", "bidi",
				"call", w.mtd.FullName())
		}
		return fmt.Errorf("makeBidiRequest is for bidi-streaming methods; %q is %s", w.mtd.FullName(), methodType(w.mtd))
	}
	sd := grpc.StreamDesc{
		StreamName:    string(w.mtd.Name()),
		ServerStreams: w.mtd.IsStreamingServer(),
		ClientStreams: w.mtd.IsStreamingClient(),
	}
	if cs, err := w.stub.NewStream(*ctx, &sd, requestMethod(w.mtd), callOptions...); err != nil {
		if w.config.hasLog {
			w.config.log.Errorw("Invoke Bidi RPC call error: "+err.Error(),
				"workerID", w.workerID, "call type", "bidi",
				"call", w.mtd.FullName(), "error", err)
		}
		return err
	} else {
		str = &BidiStream{cs, w.mtd.Input(), w.mtd.Output()}
	}

	// if err != nil {
	// 	if w.config.hasLog {
	// 		w.config.log.Errorw("Invoke Bidi RPC call error: "+err.Error(),
	// 			"workerID", w.workerID, "call type", "bidi",
	// 			"call", w.mtd.GetFullyQualifiedName(), "error", err)
	// 	}

	// 	return err
	// }

	counter := uint(0)
	indexCounter := 0
	recvDone := make(chan bool)
	sendDone := make(chan bool)

	closeStream := func() {
		closeErr := str.stream.CloseSend()

		// TODO: Should this check for error first?
		if w.config.hasLog {
			w.config.log.Debugw("Close send", "workerID", w.workerID, "call type", "bidi",
				"call", w.mtd.FullName(), "error", closeErr)
		}
	}

	doneCh := make(chan struct{})
	cancel := make(chan struct{}, 1)
	if w.config.streamCallDuration > 0 {
		go func() {
			sct := time.NewTimer(w.config.streamCallDuration)
			select {
			case <-sct.C:
				cancel <- struct{}{}
				return
			case <-doneCh:
				if !sct.Stop() {
					<-sct.C
				}
				return
			}
		}()
	}

	var recvErr error

	// Should this return err?
	go func() {
		interceptCanceled := false

		for recvErr == nil {
			resp := dynamicpb.NewMessage(str.respType)
			if recvErr = str.stream.RecvMsg(resp); recvErr != nil {
				if w.config.hasLog {
					w.config.log.Debugw("Receive message", "workerID", w.workerID, "call type", "bidi",
						"call", w.mtd.FullName(),
						"response", resp, "error", recvErr)
				}
			}

			if w.streamRecv != nil {
				//if converted, ok := res.(*dynamic.Message); ok {
				iErr := w.streamRecv(resp, recvErr)
				if errors.Is(iErr, ErrEndStream) && !interceptCanceled {
					interceptCanceled = true
					if len(cancel) == 0 {
						cancel <- struct{}{}
					}
					recvErr = nil
				}
				//}
			}

			if streamInterceptor != nil {
				//if converted, ok := res.(*dynamic.Message); ok {
				iErr := streamInterceptor.Recv(resp, recvErr)
				if errors.Is(iErr, ErrEndStream) && !interceptCanceled {
					interceptCanceled = true
					if len(cancel) == 0 {
						cancel <- struct{}{}
					}
					recvErr = nil
				}
				//}
			}

			if recvErr != nil {
				close(recvDone)
				break
			}
		}
	}()

	go func() {
		done := false

		for err == nil && !done {

			// check at start before send too
			if len(cancel) > 0 {
				<-cancel
				closeStream()
				break
			}

			// default message provider checks counter
			// but we also need to keep our own counts
			// in case of custom client providers

			var payload *dynamicpb.Message
			payload, err = messageProvider(ctd)

			isLast := false
			if errors.Is(err, ErrLastMessage) {
				isLast = true
				err = nil
			}

			if err != nil {
				if errors.Is(err, ErrEndStream) {
					err = nil
				}

				closeStream()
				break
			}

			err := checkMessageType(str.reqType, payload)

			if err != nil {
				if err == io.EOF {
					err = nil
				}

				break
			}

			str.stream.SendMsg(payload)

			if err != nil {
				if err == io.EOF {
					err = nil
				}

				break
			}

			if w.config.hasLog {
				w.config.log.Debugw("Send message", "workerID", w.workerID, "call type", "bidi",
					"call", w.mtd.FullName(),
					"payload", payload, "error", err)
			}

			if isLast {
				closeStream()
				break
			}

			counter++
			indexCounter++

			if w.config.streamCallCount > 0 && counter >= w.config.streamCallCount {
				closeStream()
				break
			}

			if len(cancel) > 0 {
				<-cancel
				closeStream()
				break
			}

			if w.config.streamInterval > 0 {
				wait := time.NewTimer(w.config.streamInterval)
				select {
				case <-wait.C:
					break
				case <-cancel:
					if !wait.Stop() {
						<-wait.C
					}
					closeStream()
					done = true
					break
				}
			}
		}

		close(sendDone)
	}()

	_, _ = <-recvDone, <-sendDone

	for len(cancel) > 0 {
		<-cancel
	}

	close(doneCh)
	close(cancel)

	if err == nil && recvErr != nil {
		err = recvErr
	}

	return err
}
