// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.24.0
// 	protoc        v3.11.4
// source: go/slave.proto

package main

import (
	context "context"
	proto "github.com/golang/protobuf/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// This is a compile-time assertion that a sufficiently up-to-date version
// of the legacy proto package is being used.
const _ = proto.ProtoPackageIsVersion4

type String struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Value string `protobuf:"bytes,1,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *String) Reset() {
	*x = String{}
	if protoimpl.UnsafeEnabled {
		mi := &file_go_slave_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *String) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*String) ProtoMessage() {}

func (x *String) ProtoReflect() protoreflect.Message {
	mi := &file_go_slave_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use String.ProtoReflect.Descriptor instead.
func (*String) Descriptor() ([]byte, []int) {
	return file_go_slave_proto_rawDescGZIP(), []int{0}
}

func (x *String) GetValue() string {
	if x != nil {
		return x.Value
	}
	return ""
}

var File_go_slave_proto protoreflect.FileDescriptor

var file_go_slave_proto_rawDesc = []byte{
	0x0a, 0x0e, 0x67, 0x6f, 0x2f, 0x73, 0x6c, 0x61, 0x76, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x12, 0x04, 0x6d, 0x61, 0x69, 0x6e, 0x22, 0x1e, 0x0a, 0x06, 0x53, 0x74, 0x72, 0x69, 0x6e, 0x67,
	0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x32, 0x74, 0x0a, 0x09, 0x4b, 0x56, 0x53, 0x65, 0x72, 0x76,
	0x69, 0x63, 0x65, 0x12, 0x21, 0x0a, 0x03, 0x50, 0x75, 0x74, 0x12, 0x0c, 0x2e, 0x6d, 0x61, 0x69,
	0x6e, 0x2e, 0x53, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x1a, 0x0c, 0x2e, 0x6d, 0x61, 0x69, 0x6e, 0x2e,
	0x53, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x12, 0x21, 0x0a, 0x03, 0x47, 0x65, 0x74, 0x12, 0x0c, 0x2e,
	0x6d, 0x61, 0x69, 0x6e, 0x2e, 0x53, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x1a, 0x0c, 0x2e, 0x6d, 0x61,
	0x69, 0x6e, 0x2e, 0x53, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x12, 0x21, 0x0a, 0x03, 0x44, 0x65, 0x6c,
	0x12, 0x0c, 0x2e, 0x6d, 0x61, 0x69, 0x6e, 0x2e, 0x53, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x1a, 0x0c,
	0x2e, 0x6d, 0x61, 0x69, 0x6e, 0x2e, 0x53, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x42, 0x0c, 0x5a, 0x0a,
	0x64, 0x73, 0x2f, 0x67, 0x6f, 0x2f, 0x6d, 0x61, 0x69, 0x6e, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x33,
}

var (
	file_go_slave_proto_rawDescOnce sync.Once
	file_go_slave_proto_rawDescData = file_go_slave_proto_rawDesc
)

func file_go_slave_proto_rawDescGZIP() []byte {
	file_go_slave_proto_rawDescOnce.Do(func() {
		file_go_slave_proto_rawDescData = protoimpl.X.CompressGZIP(file_go_slave_proto_rawDescData)
	})
	return file_go_slave_proto_rawDescData
}

var file_go_slave_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_go_slave_proto_goTypes = []interface{}{
	(*String)(nil), // 0: main.String
}
var file_go_slave_proto_depIdxs = []int32{
	0, // 0: main.KVService.Put:input_type -> main.String
	0, // 1: main.KVService.Get:input_type -> main.String
	0, // 2: main.KVService.Del:input_type -> main.String
	0, // 3: main.KVService.Put:output_type -> main.String
	0, // 4: main.KVService.Get:output_type -> main.String
	0, // 5: main.KVService.Del:output_type -> main.String
	3, // [3:6] is the sub-list for method output_type
	0, // [0:3] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_go_slave_proto_init() }
func file_go_slave_proto_init() {
	if File_go_slave_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_go_slave_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*String); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_go_slave_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_go_slave_proto_goTypes,
		DependencyIndexes: file_go_slave_proto_depIdxs,
		MessageInfos:      file_go_slave_proto_msgTypes,
	}.Build()
	File_go_slave_proto = out.File
	file_go_slave_proto_rawDesc = nil
	file_go_slave_proto_goTypes = nil
	file_go_slave_proto_depIdxs = nil
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConnInterface

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion6

// KVServiceClient is the client API for KVService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type KVServiceClient interface {
	Put(ctx context.Context, in *String, opts ...grpc.CallOption) (*String, error)
	Get(ctx context.Context, in *String, opts ...grpc.CallOption) (*String, error)
	Del(ctx context.Context, in *String, opts ...grpc.CallOption) (*String, error)
}

type kVServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewKVServiceClient(cc grpc.ClientConnInterface) KVServiceClient {
	return &kVServiceClient{cc}
}

func (c *kVServiceClient) Put(ctx context.Context, in *String, opts ...grpc.CallOption) (*String, error) {
	out := new(String)
	err := c.cc.Invoke(ctx, "/main.KVService/Put", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kVServiceClient) Get(ctx context.Context, in *String, opts ...grpc.CallOption) (*String, error) {
	out := new(String)
	err := c.cc.Invoke(ctx, "/main.KVService/Get", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kVServiceClient) Del(ctx context.Context, in *String, opts ...grpc.CallOption) (*String, error) {
	out := new(String)
	err := c.cc.Invoke(ctx, "/main.KVService/Del", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// KVServiceServer is the server API for KVService service.
type KVServiceServer interface {
	Put(context.Context, *String) (*String, error)
	Get(context.Context, *String) (*String, error)
	Del(context.Context, *String) (*String, error)
}

// UnimplementedKVServiceServer can be embedded to have forward compatible implementations.
type UnimplementedKVServiceServer struct {
}

func (*UnimplementedKVServiceServer) Put(context.Context, *String) (*String, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Put not implemented")
}
func (*UnimplementedKVServiceServer) Get(context.Context, *String) (*String, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Get not implemented")
}
func (*UnimplementedKVServiceServer) Del(context.Context, *String) (*String, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Del not implemented")
}

func RegisterKVServiceServer(s *grpc.Server, srv KVServiceServer) {
	s.RegisterService(&_KVService_serviceDesc, srv)
}

func _KVService_Put_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(String)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServiceServer).Put(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/main.KVService/Put",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServiceServer).Put(ctx, req.(*String))
	}
	return interceptor(ctx, in, info, handler)
}

func _KVService_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(String)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServiceServer).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/main.KVService/Get",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServiceServer).Get(ctx, req.(*String))
	}
	return interceptor(ctx, in, info, handler)
}

func _KVService_Del_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(String)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServiceServer).Del(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/main.KVService/Del",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServiceServer).Del(ctx, req.(*String))
	}
	return interceptor(ctx, in, info, handler)
}

var _KVService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "main.KVService",
	HandlerType: (*KVServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Put",
			Handler:    _KVService_Put_Handler,
		},
		{
			MethodName: "Get",
			Handler:    _KVService_Get_Handler,
		},
		{
			MethodName: "Del",
			Handler:    _KVService_Del_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "go/slave.proto",
}
