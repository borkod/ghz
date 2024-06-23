package protodescv2

import (
	"context"
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
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

var errNoMethodNameSpecified = errors.New("no method name specified")

// GetMethodDescFromProto gets method descriptor for the given call symbol from proto file given my path proto
// imports is used for import paths in parsing the proto file
func GetMethodDescFromProto(call, proto string, imports []string) (*descriptorpb.MethodDescriptorProto, error) {
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

	fileDesc := res.FileDescriptorProto()
	files := map[string]*descriptorpb.FileDescriptorProto{}
	files[fileDesc.GetName()] = fileDesc

	return getMethodDesc(call, files)
}

// GetMethodDescFromProtoSet gets method descriptor for the given call symbol from protoset file given my path protoset
func GetMethodDescFromProtoSet(call, protoset string) (*descriptorpb.MethodDescriptorProto, error) {
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
func GetMethodDescFromProtoSetBinary(call string, b []byte) (*descriptorpb.MethodDescriptorProto, error) {
	var fds descriptorpb.FileDescriptorSet
	err := proto.Unmarshal(b, &fds)
	if err != nil {
		return nil, fmt.Errorf("could not parse contents of protoset binary: %v", err)
	}

	// This doesn't make sense to me
	unresolved := map[string]*descriptorpb.FileDescriptorProto{}
	for _, fd := range fds.File {
		unresolved[fd.GetName()] = fd
	}

	// Bojan's resolved seems to be the same as unresolved??
	resolved := map[string]*descriptorpb.FileDescriptorProto{}
	for _, fd := range fds.File {
		_, err := resolveFileDescriptor(unresolved, resolved, fd.GetName())
		if err != nil {
			return nil, err
		}
	}

	return getMethodDesc(call, unresolved)
}

// GetMethodDescFromReflect gets method descriptor for the call from reflection using client
func GetMethodDescFromReflect(call string, client *grpcreflect.Client) (*descriptorpb.MethodDescriptorProto, error) {
	call = strings.Replace(call, "/", ".", -1)
	pkg, svc, _, err := parsePkgServiceMethod(call)
	stream := client.NewStream(context.Background())
	defer stream.Close()
	fileResults, err := stream.FileContainingSymbol(protoreflect.FullName(pkg + "." + svc))
	if err != nil || fileResults == nil {
		return nil, reflectionSupport(err)
	}

	files := map[string]*descriptorpb.FileDescriptorProto{}
	for _, file := range fileResults {
		files[file.GetName()] = file
	}

	return getMethodDesc(call, files)
}

func resolveFileDescriptor(unresolved map[string]*descriptorpb.FileDescriptorProto, resolved map[string]*descriptorpb.FileDescriptorProto, filename string) (*descriptorpb.FileDescriptorProto, error) {
	if r, ok := resolved[filename]; ok {
		return r, nil
	}
	fd, ok := unresolved[filename]
	if !ok {
		return nil, fmt.Errorf("no descriptor found for %q", filename)
	}
	deps := make([]*descriptorpb.FileDescriptorProto, 0, len(fd.GetDependency()))
	for _, dep := range fd.GetDependency() {
		depFd, err := resolveFileDescriptor(unresolved, resolved, dep)
		if err != nil {
			return nil, err
		}
		deps = append(deps, depFd)
	}
	// func ToFileDescriptorProto(file protoreflect.FileDescriptor) *descriptorpb.FileDescriptorProto
	// result, err := descriptorpb.CreateFileDescriptor(fd, deps...)
	// if err != nil {
	// 	return nil, err
	// }
	//resolved[filename] = result
	return nil, nil
	//return result, nil
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

func getMethodDesc(call string, files map[string]*descriptorpb.FileDescriptorProto) (*descriptorpb.MethodDescriptorProto, error) {
	_, svc, mth, err := parsePkgServiceMethod(call)
	if err != nil {
		return nil, err
	}

	for _, fd := range files {
		sd, err := findService(fd, svc)
		if err != nil {
			continue
		}
		return findMethod(sd, mth)
	}
	return nil, fmt.Errorf("method %q not found", mth)
}

func findService(fd *descriptorpb.FileDescriptorProto, fullyQualifiedName string) (*descriptorpb.ServiceDescriptorProto, error) {
	for _, svc := range fd.GetService() {
		if svc.GetName() == fullyQualifiedName {
			return svc, nil
		}
	}
	return nil, fmt.Errorf("service %q not found", fullyQualifiedName)
}

func findMethod(sd *descriptorpb.ServiceDescriptorProto, methodName string) (*descriptorpb.MethodDescriptorProto, error) {
	for _, mth := range sd.GetMethod() {
		if mth.GetName() == methodName {
			return mth, nil
		}
	}
	return nil, fmt.Errorf("method %q not found", methodName)
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
