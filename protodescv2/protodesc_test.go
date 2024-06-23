package protodescv2

import (
	"crypto/tls"
	"net"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/grpcreflect"
	"github.com/stretchr/testify/assert"

	"github.com/bojand/ghz/internal"

	"golang.org/x/net/http2"
)

func TestProtodesc_GetMethodDescFromProto(t *testing.T) {
	t.Run("invalid path", func(t *testing.T) {
		md, err := GetMethodDescFromProto("pkg.Call", "invalid.proto", []string{})
		assert.Error(t, err)
		assert.Nil(t, md)
	})

	t.Run("invalid call symbol", func(t *testing.T) {
		md, err := GetMethodDescFromProto("pkg.Call", "../testdata/greeter.proto", []string{})
		assert.Error(t, err)
		assert.Nil(t, md)
	})

	t.Run("invalid package", func(t *testing.T) {
		md, err := GetMethodDescFromProto("helloworld.pkg.SayHello", "../testdata/greeter.proto", []string{})
		assert.Error(t, err)
		assert.Nil(t, md)
	})

	t.Run("invalid method", func(t *testing.T) {
		md, err := GetMethodDescFromProto("helloworld.Greeter.Foo", "../testdata/greeter.proto", []string{})
		assert.Error(t, err)
		assert.Nil(t, md)
	})

	t.Run("valid symbol", func(t *testing.T) {
		md, err := GetMethodDescFromProto("helloworld.Greeter.SayHello", "../testdata/greeter.proto", []string{})
		assert.NoError(t, err)
		assert.NotNil(t, md)
	})

	t.Run("valid symbol slashes", func(t *testing.T) {
		md, err := GetMethodDescFromProto("helloworld.Greeter/SayHello", "../testdata/greeter.proto", []string{})
		assert.NoError(t, err)
		assert.NotNil(t, md)
	})

	t.Run("proto3 optional support", func(t *testing.T) {
		md, err := GetMethodDescFromProto("helloworld.OptionalGreeter/SayHello", "../testdata/optional.proto", []string{})
		assert.NoError(t, err)
		assert.NotNil(t, md)
	})
}
func TestParseServiceMethod(t *testing.T) {
	testParseServiceMethodSuccess(t, "package.Service.Method", "package", "Service", "Method")
	testParseServiceMethodSuccess(t, ".package.Service.Method", "package", "Service", "Method")
	testParseServiceMethodSuccess(t, "package.Service/Method", "package", "Service", "Method")
	testParseServiceMethodSuccess(t, ".package.Service/Method", "package", "Service", "Method")
	testParseServiceMethodSuccess(t, "rpc.package.v1.Service.Method", "rpc.package.v1", "Service", "Method")
	testParseServiceMethodSuccess(t, "rpc.package.v1.Service/Method", "rpc.package.v1", "Service", "Method")
	// THESE don't align with valid inputs in the main.go ??
	// testParseServiceMethodSuccess(t, "Service.Method", "", "Service", "Method")
	// testParseServiceMethodSuccess(t, ".Service.Method", "", "Service", "Method")
	// testParseServiceMethodSuccess(t, "Service/Method", "", "Service", "Method")
	// testParseServiceMethodSuccess(t, ".Service/Method", "", "Service", "Method")
	// testParseServiceMethodError(t, "")
	// testParseServiceMethodError(t, ".")
	// testParseServiceMethodError(t, "package/Service/Method")
}

func testParseServiceMethodSuccess(t *testing.T, svcAndMethod string, expectedPkg string, expectedService string, expectedMethod string) {
	pkg, service, method, err := parsePkgServiceMethod(svcAndMethod)
	assert.NoError(t, err)
	assert.Equal(t, expectedPkg, pkg)
	assert.Equal(t, expectedService, service)
	assert.Equal(t, expectedMethod, method)
}

func testParseServiceMethodError(t *testing.T, svcAndMethod string) {
	_, _, _, err := parsePkgServiceMethod(svcAndMethod)
	assert.Error(t, err)
}

func TestProtodesc_GetMethodDescFromProtoSet(t *testing.T) {
	t.Run("invalid path", func(t *testing.T) {
		md, err := GetMethodDescFromProtoSet("pkg.Call", "invalid.protoset")
		assert.Error(t, err)
		assert.Nil(t, md)
	})

	t.Run("invalid call symbol", func(t *testing.T) {
		md, err := GetMethodDescFromProtoSet("pkg.Call", "../testdata/bundle.protoset")
		assert.Error(t, err)
		assert.Nil(t, md)
	})

	t.Run("invalid package", func(t *testing.T) {
		md, err := GetMethodDescFromProtoSet("helloworld.pkg.SayHello", "../testdata/bundle.protoset")
		assert.Error(t, err)
		assert.Nil(t, md)
	})

	t.Run("invalid method", func(t *testing.T) {
		md, err := GetMethodDescFromProtoSet("helloworld.Greeter.Foo", "../testdata/bundle.protoset")
		assert.Error(t, err)
		assert.Nil(t, md)
	})

	t.Run("valid symbol", func(t *testing.T) {
		md, err := GetMethodDescFromProtoSet("helloworld.Greeter.SayHello", "../testdata/bundle.protoset")
		assert.NoError(t, err)
		assert.NotNil(t, md)
	})

	t.Run("valid symbol proto 2", func(t *testing.T) {
		md, err := GetMethodDescFromProtoSet("cap.Capper.Cap", "../testdata/bundle.protoset")
		assert.NoError(t, err)
		assert.NotNil(t, md)
	})

	t.Run("valid symbol slashes", func(t *testing.T) {
		md, err := GetMethodDescFromProtoSet("helloworld.Greeter/SayHello", "../testdata/bundle.protoset")
		assert.NoError(t, err)
		assert.NotNil(t, md)
	})
}

func TestProtodesc_GetMethodDescFromReflect(t *testing.T) {
	_, s, err := internal.StartServer(false)

	if err != nil {
		assert.FailNow(t, err.Error())
	}

	defer s.Stop()

	t.Run("test connect rpc demo", func(t *testing.T) {
		refClient := grpcreflect.NewClient(http.DefaultClient, "https://demo.connectrpc.com")

		mtd, err := GetMethodDescFromReflect("connectrpc.eliza.v1.ElizaService.Say", refClient)
		assert.NoError(t, err)
		assert.NotNil(t, mtd)
		assert.Equal(t, "Say", mtd.GetName())
	})

	t.Run("test known call", func(t *testing.T) {
		refClient := grpcreflect.NewClient(newInsecureClient(), "http://"+internal.TestLocalhost, connect.WithGRPC())

		mtd, err := GetMethodDescFromReflect("helloworld.Greeter.SayHello", refClient)
		assert.NoError(t, err)
		assert.NotNil(t, mtd)
		assert.Equal(t, "SayHello", mtd.GetName())
	})

	t.Run("test known call with /", func(t *testing.T) {
		refClient := grpcreflect.NewClient(newInsecureClient(), "http://"+internal.TestLocalhost, connect.WithGRPC())

		mtd, err := GetMethodDescFromReflect("helloworld.Greeter/SayHello", refClient)
		assert.NoError(t, err)
		assert.NotNil(t, mtd)
		assert.Equal(t, "SayHello", mtd.GetName())
	})

	t.Run("test unknown known call", func(t *testing.T) {
		refClient := grpcreflect.NewClient(newInsecureClient(), "http://"+internal.TestLocalhost, connect.WithGRPC())

		mtd, err := GetMethodDescFromReflect("helloworld.Greeter/SayHelloAsdf", refClient)
		assert.Error(t, err)
		assert.Nil(t, mtd)
	})
}

func newInsecureClient() *http.Client {
	return &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLS: func(network, addr string, _ *tls.Config) (net.Conn, error) {
				// If you're also using this client for non-h2c traffic, you may want
				// to delegate to tls.Dial if the network isn't TCP or the addr isn't
				// in an allowlist.
				return net.Dial(network, addr)
			},
			IdleConnTimeout: 10 * time.Second,
		},
	}
}
