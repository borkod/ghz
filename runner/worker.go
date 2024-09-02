package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic/grpcdynamic"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// TickValue is the tick value
type TickValue struct {
	instant   time.Time
	reqNumber uint64
}

// Worker is used for doing a single stream of requests in parallel
type Worker struct {
	stub grpcdynamic.Stub
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

	fd, err := desc.CreateFileDescriptor(protodesc.ToFileDescriptorProto(w.mtd.ParentFile()))
	if err != nil {
		// TODO: perform w.config.log.debugw
		return err
	}
	mtd := fd.FindService(string(w.mtd.Parent().FullName())).FindMethodByName(string(w.mtd.Name()))
	resv1, resErr := w.stub.InvokeRpc(*ctx, mtd, input, callOptions...)
	res := protoadapt.MessageV2Of(resv1)

	if w.config.hasLog {
		// TODO: Make sure no errors here
		inputData, _ := proto.Marshal(input)
		//inputData, _ := input.MarshalJSON()
		resData, _ := json.Marshal(res)

		w.config.log.Debugw("Received response", "workerID", w.workerID, "call type", "unary",
			"call", w.mtd.FullName(),
			"input", string(inputData), "metadata", reqMD,
			"response", string(resData), "error", resErr)
	}

	return resErr
}

func (w *Worker) makeClientStreamingRequest(ctx *context.Context,
	ctd *CallData, messageProvider StreamMessageProviderFunc) error {
	var str *grpcdynamic.ClientStream
	var callOptions = []grpc.CallOption{}
	if w.config.enableCompression {
		callOptions = append(callOptions, grpc.UseCompressor(gzip.Name))
	}

	fd, err := desc.CreateFileDescriptor(protodesc.ToFileDescriptorProto(w.mtd.ParentFile()))
	if err != nil {
		// TODO: perform w.config.log.debugw
		return err
	}
	mtd := fd.FindService(string(w.mtd.Parent().FullName())).FindMethodByName(string(w.mtd.Name()))

	str, err = w.stub.InvokeRpcClientStream(*ctx, mtd, callOptions...)
	if err != nil {
		if w.config.hasLog {
			w.config.log.Errorw("Invoke Client Streaming RPC call error: "+err.Error(), "workerID", w.workerID,
				"call type", "client-streaming",
				"call", w.mtd.FullName(), "error", err)
		}
		return err
	}

	// TODO: should this return error? right now we just log errors?
	closeStream := func() {
		res, closeErr := str.CloseAndReceive()

		if w.config.hasLog {
			w.config.log.Debugw("Close and receive", "workerID", w.workerID, "call type", "client-streaming",
				"call", w.mtd.FullName(),
				"response", res, "error", closeErr)
		}
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
		return fmt.Errorf("InvokeRpcServerStream is for server-streaming methods; %q is %s", w.mtd.Name(), methodType(w.mtd))
	}
	if err := checkMessageType(w.mtd.Input(), input); err != nil {
		return err
	}

	var callOptions = []grpc.CallOption{}
	if w.config.enableCompression {
		callOptions = append(callOptions, grpc.UseCompressor(gzip.Name))
	}

	callCtx, callCancel := context.WithCancel(*ctx)
	defer callCancel()

	fd, err := desc.CreateFileDescriptor(protodesc.ToFileDescriptorProto(w.mtd.ParentFile()))
	if err != nil {
		// TODO: perform w.config.log.debugw
		return err
	}
	mtd := fd.FindService(string(w.mtd.Parent().FullName())).FindMethodByName(string(w.mtd.Name()))

	str, err := w.stub.InvokeRpcServerStream(callCtx, mtd, input, callOptions...)

	if err != nil {
		if w.config.hasLog {
			w.config.log.Errorw("Invoke Server Streaming RPC call error: "+err.Error(), "workerID", w.workerID,
				"call type", "server-streaming",
				"call", w.mtd.Name(),
				"input", input, "error", err)
		}

		return err
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

	interceptCanceled := false
	counter := uint(0)
	for err == nil {
		// we should check before receiving a message too
		if w.config.streamCallDuration > 0 && len(cancel) > 0 {
			<-cancel
			callCancel()
			break
		}

		//resp := dynamicpb.NewMessage(respType)
		res, err := str.RecvMsg()
		if w.config.hasLog {
			w.config.log.Debugw("Receive message", "workerID", w.workerID, "call type", "server-streaming",
				"call", w.mtd.Name(),
				"response", res, "error", err)
		}

		// with any of the cancellation operations we can't just bail
		// we have to drain the messages until the server gets the cancel and ends their side of the stream

		if w.streamRecv != nil {
			if converted, ok := res.(*dynamicpb.Message); ok {
				err = w.streamRecv(converted, err)
				if errors.Is(err, ErrEndStream) && !interceptCanceled {
					interceptCanceled = true
					err = nil

					callCancel()
				}
			}
		}

		if streamInterceptor != nil {
			if converted, ok := res.(*dynamicpb.Message); ok {
				err = streamInterceptor.Recv(converted, err)
				if errors.Is(err, ErrEndStream) && !interceptCanceled {
					interceptCanceled = true
					err = nil

					callCancel()
				}
			}
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

	if !w.mtd.IsStreamingClient() || !w.mtd.IsStreamingServer() {
		if w.config.hasLog {
			w.config.log.Errorw("makeBidiRequest is for bidi-streaming methods; %q is %s", w.mtd.FullName(), methodType(w.mtd),
				"workerID", w.workerID, "call type", "bidi",
				"call", w.mtd.FullName())
		}
		return fmt.Errorf("makeBidiRequest is for bidi-streaming methods; %q is %s", w.mtd.FullName(), methodType(w.mtd))
	}

	fd, err := desc.CreateFileDescriptor(protodesc.ToFileDescriptorProto(w.mtd.ParentFile()))
	if err != nil {
		// TODO: perform w.config.log.debugw
		return err
	}
	mtd := fd.FindService(string(w.mtd.Parent().FullName())).FindMethodByName(string(w.mtd.Name()))

	str, err := w.stub.InvokeRpcBidiStream(*ctx, mtd, callOptions...)

	if err != nil {
		if w.config.hasLog {
			w.config.log.Errorw("Invoke Bidi RPC call error: "+err.Error(),
				"workerID", w.workerID, "call type", "bidi",
				"call", w.mtd.FullName(), "error", err)
		}

		return err
	}

	counter := uint(0)
	indexCounter := 0
	recvDone := make(chan bool)
	sendDone := make(chan bool)

	closeStream := func() {
		closeErr := str.CloseSend()

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
			//var res proto.Message
			//resp := dynamicpb.NewMessage(respType)
			resv1, recvErr := str.RecvMsg()
			if w.config.hasLog {
				w.config.log.Debugw("Receive message", "workerID", w.workerID, "call type", "bidi",
					"call", w.mtd.FullName(),
					"response", resv1, "error", recvErr)
			}

			if w.streamRecv != nil {
				//if converted, ok := res.(*dynamic.Message); ok {
				res := dynamicpb.NewMessage(protoadapt.MessageV2Of(resv1).ProtoReflect().Descriptor())
				iErr := w.streamRecv(res, recvErr)
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
				res := dynamicpb.NewMessage(protoadapt.MessageV2Of(resv1).ProtoReflect().Descriptor())

				iErr := streamInterceptor.Recv(res, recvErr)
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

			err = str.SendMsg(payload)
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
