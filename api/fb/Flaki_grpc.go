//Generated by gRPC Go plugin
//If you make any local changes, they will be lost
//source: flaki

package fb

import "github.com/google/flatbuffers/go"

import (
  context "golang.org/x/net/context"
  grpc "google.golang.org/grpc"
)

// Client API for Flaki service
type FlakiClient interface{
  NextID(ctx context.Context, in *flatbuffers.Builder, 
  	opts... grpc.CallOption) (* FlakiReply, error)  
  NextValidID(ctx context.Context, in *flatbuffers.Builder, 
  	opts... grpc.CallOption) (* FlakiReply, error)  
}

type flakiClient struct {
  cc *grpc.ClientConn
}

func NewFlakiClient(cc *grpc.ClientConn) FlakiClient {
  return &flakiClient{cc}
}

func (c *flakiClient) NextID(ctx context.Context, in *flatbuffers.Builder, 
	opts... grpc.CallOption) (* FlakiReply, error) {
  out := new(FlakiReply)
  err := grpc.Invoke(ctx, "/fb.Flaki/NextID", in, out, c.cc, opts...)
  if err != nil { return nil, err }
  return out, nil
}

func (c *flakiClient) NextValidID(ctx context.Context, in *flatbuffers.Builder, 
	opts... grpc.CallOption) (* FlakiReply, error) {
  out := new(FlakiReply)
  err := grpc.Invoke(ctx, "/fb.Flaki/NextValidID", in, out, c.cc, opts...)
  if err != nil { return nil, err }
  return out, nil
}

// Server API for Flaki service
type FlakiServer interface {
  NextID(context.Context, *FlakiRequest) (*flatbuffers.Builder, error)  
  NextValidID(context.Context, *FlakiRequest) (*flatbuffers.Builder, error)  
}

func RegisterFlakiServer(s *grpc.Server, srv FlakiServer) {
  s.RegisterService(&_Flaki_serviceDesc, srv)
}

func _Flaki_NextID_Handler(srv interface{}, ctx context.Context,
	dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
  in := new(FlakiRequest)
  if err := dec(in); err != nil { return nil, err }
  if interceptor == nil { return srv.(FlakiServer).NextID(ctx, in) }
  info := &grpc.UnaryServerInfo{
    Server: srv,
    FullMethod: "/fb.Flaki/NextID",
  }
  
  handler := func(ctx context.Context, req interface{}) (interface{}, error) {
    return srv.(FlakiServer).NextID(ctx, req.(* FlakiRequest))
  }
  return interceptor(ctx, in, info, handler)
}


func _Flaki_NextValidID_Handler(srv interface{}, ctx context.Context,
	dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
  in := new(FlakiRequest)
  if err := dec(in); err != nil { return nil, err }
  if interceptor == nil { return srv.(FlakiServer).NextValidID(ctx, in) }
  info := &grpc.UnaryServerInfo{
    Server: srv,
    FullMethod: "/fb.Flaki/NextValidID",
  }
  
  handler := func(ctx context.Context, req interface{}) (interface{}, error) {
    return srv.(FlakiServer).NextValidID(ctx, req.(* FlakiRequest))
  }
  return interceptor(ctx, in, info, handler)
}


var _Flaki_serviceDesc = grpc.ServiceDesc{
  ServiceName: "fb.Flaki",
  HandlerType: (*FlakiServer)(nil),
  Methods: []grpc.MethodDesc{
    {
      MethodName: "NextID",
      Handler: _Flaki_NextID_Handler, 
    },
    {
      MethodName: "NextValidID",
      Handler: _Flaki_NextValidID_Handler, 
    },
  },
  Streams: []grpc.StreamDesc{
  },
}

