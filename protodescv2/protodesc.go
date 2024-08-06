package protodescv2

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"connectrpc.com/grpcreflect"
	"github.com/bufbuild/protocompile/parser"
	"github.com/bufbuild/protocompile/reporter"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

var errNoMethodNameSpecified = errors.New("no method name specified")

// GetMethodDescFromProto gets method descriptor for the given call symbol from proto file given my path proto
// imports is used for import paths in parsing the proto file
func GetMethodDescFromProto(call, proto string, imports []string) (protoreflect.MethodDescriptor, error) {
	er := func(err reporter.ErrorWithPos) error {
		return err.Unwrap()
	}

	filename := proto
	if filepath.IsAbs(filename) {
		filename = filepath.Base(proto)
	}

	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	rep := reporter.NewReporter(er, nil)

	ast, err := parser.Parse(filename, f, reporter.NewHandler(rep))
	if err != nil {
		return nil, err
	}

	res, err := parser.ResultFromAST(ast, true, reporter.NewHandler(rep))
	if err != nil {
		return nil, err
	}

	file := res.FileDescriptorProto()
	// files := map[string]*descriptorpb.FileDescriptorProto{}
	// files[file.GetName()] = file

	var registry protoregistry.Files

	fileDescriptor, err := protodesc.NewFile(file, &registry)
	if err != nil {
		return nil, fmt.Errorf("failed to process %q: %v", file.GetName(), err)
	}
	if err := registry.RegisterFile(fileDescriptor); err != nil {
		return nil, fmt.Errorf("failed to register %q: %v", file.GetName(), err)
	}

	return getMethodDesc(call, registry)
}

// GetMethodDescFromProtoSet gets method descriptor for the given call symbol from protoset file given my path protoset
func GetMethodDescFromProtoSet(call, protoset string) (protoreflect.MethodDescriptor, error) {
	b, err := os.ReadFile(protoset)
	if err != nil {
		return nil, fmt.Errorf("could not load protoset file %q: %v", protoset, err)
	}

	res, err := GetMethodDescFromProtoSetBinary(call, b)
	if err != nil && strings.Contains(err.Error(), "could not parse contents of protoset binary") {
		return nil, fmt.Errorf("could not parse contents of protoset file %q: %v", protoset, err)
	}

	return res, err
}

// GetMethodDescFromProtoSetBinary gets method descriptor for the given call symbol from protoset binary
func GetMethodDescFromProtoSetBinary(call string, b []byte) (protoreflect.MethodDescriptor, error) {
	var files descriptorpb.FileDescriptorSet
	err := proto.Unmarshal(b, &files)
	if err != nil {
		return nil, fmt.Errorf("could not parse contents of protoset binary: %v", err)
	}

	var registry protoregistry.Files
	for _, file := range files.File {
		fileDescriptor, err := protodesc.NewFile(file, &registry)
		if err != nil {
			return nil, fmt.Errorf("failed to process %q: %v", file.GetName(), err)
		}
		if err := registry.RegisterFile(fileDescriptor); err != nil {
			return nil, fmt.Errorf("failed to register %q: %v", file.GetName(), err)
		}
	}

	return getMethodDesc(call, registry)
}

// GetMethodDescFromReflect gets method descriptor for the call from reflection using client
func GetMethodDescFromReflect(call string, stream *grpcreflect.ClientStream) (protoreflect.MethodDescriptor, error) {
	call = strings.Replace(call, "/", ".", -1)
	pkg, svc, _, err := parsePkgServiceMethod(call)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %q: %v", call, err)
	}

	fileResults, err := stream.FileContainingSymbol(protoreflect.FullName(pkg + "." + svc))
	if err != nil || fileResults == nil {
		return nil, reflectionSupport(err)
	}

	var registry protoregistry.Files
	for _, file := range fileResults {
		fileDescriptor, err := protodesc.NewFile(file, &registry)
		if err != nil {
			return nil, fmt.Errorf("failed to process %q: %v", file.GetName(), err)
		}
		if err := registry.RegisterFile(fileDescriptor); err != nil {
			return nil, fmt.Errorf("failed to register %q: %v", file.GetName(), err)
		}
	}

	return getMethodDesc(call, registry)
}

// parsePkgServiceMethod parses the fully-qualified service name without a leading "."
// and the method name from the input string.
//
// valid inputs:
//
//	package.Service.Method
//	.package.Service.Method
//	package.Service/Method
//	.package.Service/Method
func parsePkgServiceMethod(svcAndMethod string) (string, string, string, error) {
	if len(svcAndMethod) == 0 {
		return "", "", "", errNoMethodNameSpecified
	}
	if svcAndMethod[0] == '.' {
		svcAndMethod = svcAndMethod[1:]
	}
	if len(svcAndMethod) == 0 {
		return "", "", "", errNoMethodNameSpecified
	}
	if !protoreflect.FullName(strings.Replace(svcAndMethod, "/", ".", -1)).IsValid() {
		return "", "", "", newInvalidMethodNameError(svcAndMethod)
	}
	if strings.Count(svcAndMethod, "/") > 1 {
		return "", "", "", newInvalidMethodNameError(svcAndMethod)
	} else if strings.Count(svcAndMethod, "/") == 0 {
		pos := strings.LastIndex(svcAndMethod, ".")
		if pos < 0 {
			return "", "", "", newInvalidMethodNameError(svcAndMethod)
		}
		// replace the last "." with "/"
		svcAndMethod = svcAndMethod[:pos] + "/" + svcAndMethod[pos+1:]
	}
	split := strings.Split(svcAndMethod, "/")
	method := split[1]
	pos := strings.LastIndex(split[0], ".")
	if pos < 0 {
		return "", "", "", newInvalidMethodNameError(svcAndMethod)
	}
	return split[0][:pos], split[0][pos+1:], method, nil
}

func newInvalidMethodNameError(svcAndMethod string) error {
	return fmt.Errorf("method name must be package.Service.Method or package.Service/Method: %q", svcAndMethod)
}

func getMethodDesc(call string, registry protoregistry.Files) (protoreflect.MethodDescriptor, error) {
	pkg, svc, mth, err := parsePkgServiceMethod(call)
	if err != nil {
		return nil, err
	}

	fqn := protoreflect.FullName(pkg + "." + svc + "." + mth)

	descriptor, err := registry.FindDescriptorByName(fqn)

	if err != nil {
		return nil, fmt.Errorf("failed to find method %q in given descriptors: %v", fqn, err)
	}
	methodDescriptor, ok := descriptor.(protoreflect.MethodDescriptor)
	if !ok {
		return nil, fmt.Errorf("element name %q is not a method: %v", fqn, err)
	}

	return methodDescriptor, nil
}

func reflectionSupport(err error) error {
	if err == nil {
		return nil
	}
	if stat, ok := status.FromError(err); ok && stat.Code() == codes.Unimplemented {
		return errors.New("server does not support the reflection API")
	}
	return err
}
