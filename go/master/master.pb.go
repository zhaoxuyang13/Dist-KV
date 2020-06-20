// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.24.0
// 	protoc        v3.11.4
// source: go/master/master.proto

package master

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

//message ServerConf{
//  string ip = 1;
//  int32 port = 2;
//  string name = 3;
//}
type JoinRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	//map from  gid ->  server_names
	NewGroups map[int32]*JoinRequest_ServerConfs `protobuf:"bytes,1,rep,name=newGroups,proto3" json:"newGroups,omitempty" protobuf_key:"varint,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *JoinRequest) Reset() {
	*x = JoinRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_go_master_master_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *JoinRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*JoinRequest) ProtoMessage() {}

func (x *JoinRequest) ProtoReflect() protoreflect.Message {
	mi := &file_go_master_master_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use JoinRequest.ProtoReflect.Descriptor instead.
func (*JoinRequest) Descriptor() ([]byte, []int) {
	return file_go_master_master_proto_rawDescGZIP(), []int{0}
}

func (x *JoinRequest) GetNewGroups() map[int32]*JoinRequest_ServerConfs {
	if x != nil {
		return x.NewGroups
	}
	return nil
}

type LeaveRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	GroupIDs []int32 `protobuf:"varint,1,rep,packed,name=GroupIDs,proto3" json:"GroupIDs,omitempty"`
}

func (x *LeaveRequest) Reset() {
	*x = LeaveRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_go_master_master_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LeaveRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LeaveRequest) ProtoMessage() {}

func (x *LeaveRequest) ProtoReflect() protoreflect.Message {
	mi := &file_go_master_master_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LeaveRequest.ProtoReflect.Descriptor instead.
func (*LeaveRequest) Descriptor() ([]byte, []int) {
	return file_go_master_master_proto_rawDescGZIP(), []int{1}
}

func (x *LeaveRequest) GetGroupIDs() []int32 {
	if x != nil {
		return x.GroupIDs
	}
	return nil
}

type QueryRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ConfVersion int32 `protobuf:"varint,1,opt,name=confVersion,proto3" json:"confVersion,omitempty"`
}

func (x *QueryRequest) Reset() {
	*x = QueryRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_go_master_master_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *QueryRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*QueryRequest) ProtoMessage() {}

func (x *QueryRequest) ProtoReflect() protoreflect.Message {
	mi := &file_go_master_master_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use QueryRequest.ProtoReflect.Descriptor instead.
func (*QueryRequest) Descriptor() ([]byte, []int) {
	return file_go_master_master_proto_rawDescGZIP(), []int{2}
}

func (x *QueryRequest) GetConfVersion() int32 {
	if x != nil {
		return x.ConfVersion
	}
	return 0
}

type Empty struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *Empty) Reset() {
	*x = Empty{}
	if protoimpl.UnsafeEnabled {
		mi := &file_go_master_master_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Empty) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Empty) ProtoMessage() {}

func (x *Empty) ProtoReflect() protoreflect.Message {
	mi := &file_go_master_master_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Empty.ProtoReflect.Descriptor instead.
func (*Empty) Descriptor() ([]byte, []int) {
	return file_go_master_master_proto_rawDescGZIP(), []int{3}
}

type Conf struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Version  int32 `protobuf:"varint,1,opt,name=version,proto3" json:"version,omitempty"`
	ShardNum int32 `protobuf:"varint,2,opt,name=shardNum,proto3" json:"shardNum,omitempty"`
	// map from a gid -> Group (this map contains all groups in his replica
	Id2Group map[int32]*Conf_Group `protobuf:"bytes,3,rep,name=id2group,proto3" json:"id2group,omitempty" protobuf_key:"varint,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	//map from a replicaId -> gid (valid assignments for all 0 ~ shardNum-1
	Assignment map[int32]int32 `protobuf:"bytes,4,rep,name=assignment,proto3" json:"assignment,omitempty" protobuf_key:"varint,1,opt,name=key,proto3" protobuf_val:"varint,2,opt,name=value,proto3"`
}

func (x *Conf) Reset() {
	*x = Conf{}
	if protoimpl.UnsafeEnabled {
		mi := &file_go_master_master_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Conf) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Conf) ProtoMessage() {}

func (x *Conf) ProtoReflect() protoreflect.Message {
	mi := &file_go_master_master_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Conf.ProtoReflect.Descriptor instead.
func (*Conf) Descriptor() ([]byte, []int) {
	return file_go_master_master_proto_rawDescGZIP(), []int{4}
}

func (x *Conf) GetVersion() int32 {
	if x != nil {
		return x.Version
	}
	return 0
}

func (x *Conf) GetShardNum() int32 {
	if x != nil {
		return x.ShardNum
	}
	return 0
}

func (x *Conf) GetId2Group() map[int32]*Conf_Group {
	if x != nil {
		return x.Id2Group
	}
	return nil
}

func (x *Conf) GetAssignment() map[int32]int32 {
	if x != nil {
		return x.Assignment
	}
	return nil
}

type JoinRequest_ServerConfs struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Names []string `protobuf:"bytes,1,rep,name=names,proto3" json:"names,omitempty"`
}

func (x *JoinRequest_ServerConfs) Reset() {
	*x = JoinRequest_ServerConfs{}
	if protoimpl.UnsafeEnabled {
		mi := &file_go_master_master_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *JoinRequest_ServerConfs) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*JoinRequest_ServerConfs) ProtoMessage() {}

func (x *JoinRequest_ServerConfs) ProtoReflect() protoreflect.Message {
	mi := &file_go_master_master_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use JoinRequest_ServerConfs.ProtoReflect.Descriptor instead.
func (*JoinRequest_ServerConfs) Descriptor() ([]byte, []int) {
	return file_go_master_master_proto_rawDescGZIP(), []int{0, 0}
}

func (x *JoinRequest_ServerConfs) GetNames() []string {
	if x != nil {
		return x.Names
	}
	return nil
}

type Conf_Group struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	GroupID int32    `protobuf:"varint,1,opt,name=groupID,proto3" json:"groupID,omitempty"`
	Servers []string `protobuf:"bytes,2,rep,name=servers,proto3" json:"servers,omitempty"`
	Shards  []int32  `protobuf:"varint,3,rep,packed,name=shards,proto3" json:"shards,omitempty"`
}

func (x *Conf_Group) Reset() {
	*x = Conf_Group{}
	if protoimpl.UnsafeEnabled {
		mi := &file_go_master_master_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Conf_Group) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Conf_Group) ProtoMessage() {}

func (x *Conf_Group) ProtoReflect() protoreflect.Message {
	mi := &file_go_master_master_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Conf_Group.ProtoReflect.Descriptor instead.
func (*Conf_Group) Descriptor() ([]byte, []int) {
	return file_go_master_master_proto_rawDescGZIP(), []int{4, 0}
}

func (x *Conf_Group) GetGroupID() int32 {
	if x != nil {
		return x.GroupID
	}
	return 0
}

func (x *Conf_Group) GetServers() []string {
	if x != nil {
		return x.Servers
	}
	return nil
}

func (x *Conf_Group) GetShards() []int32 {
	if x != nil {
		return x.Shards
	}
	return nil
}

var File_go_master_master_proto protoreflect.FileDescriptor

var file_go_master_master_proto_rawDesc = []byte{
	0x0a, 0x16, 0x67, 0x6f, 0x2f, 0x6d, 0x61, 0x73, 0x74, 0x65, 0x72, 0x2f, 0x6d, 0x61, 0x73, 0x74,
	0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x06, 0x6d, 0x61, 0x73, 0x74, 0x65, 0x72,
	0x22, 0xd3, 0x01, 0x0a, 0x0b, 0x4a, 0x6f, 0x69, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x12, 0x40, 0x0a, 0x09, 0x6e, 0x65, 0x77, 0x47, 0x72, 0x6f, 0x75, 0x70, 0x73, 0x18, 0x01, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x22, 0x2e, 0x6d, 0x61, 0x73, 0x74, 0x65, 0x72, 0x2e, 0x4a, 0x6f, 0x69,
	0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x2e, 0x4e, 0x65, 0x77, 0x47, 0x72, 0x6f, 0x75,
	0x70, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x09, 0x6e, 0x65, 0x77, 0x47, 0x72, 0x6f, 0x75,
	0x70, 0x73, 0x1a, 0x23, 0x0a, 0x0b, 0x53, 0x65, 0x72, 0x76, 0x65, 0x72, 0x43, 0x6f, 0x6e, 0x66,
	0x73, 0x12, 0x14, 0x0a, 0x05, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x09,
	0x52, 0x05, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x1a, 0x5d, 0x0a, 0x0e, 0x4e, 0x65, 0x77, 0x47, 0x72,
	0x6f, 0x75, 0x70, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x35, 0x0a, 0x05, 0x76,
	0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1f, 0x2e, 0x6d, 0x61, 0x73,
	0x74, 0x65, 0x72, 0x2e, 0x4a, 0x6f, 0x69, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x2e,
	0x53, 0x65, 0x72, 0x76, 0x65, 0x72, 0x43, 0x6f, 0x6e, 0x66, 0x73, 0x52, 0x05, 0x76, 0x61, 0x6c,
	0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x22, 0x2a, 0x0a, 0x0c, 0x4c, 0x65, 0x61, 0x76, 0x65, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1a, 0x0a, 0x08, 0x47, 0x72, 0x6f, 0x75, 0x70, 0x49,
	0x44, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x05, 0x52, 0x08, 0x47, 0x72, 0x6f, 0x75, 0x70, 0x49,
	0x44, 0x73, 0x22, 0x30, 0x0a, 0x0c, 0x51, 0x75, 0x65, 0x72, 0x79, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x12, 0x20, 0x0a, 0x0b, 0x63, 0x6f, 0x6e, 0x66, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f,
	0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0b, 0x63, 0x6f, 0x6e, 0x66, 0x56, 0x65, 0x72,
	0x73, 0x69, 0x6f, 0x6e, 0x22, 0x07, 0x0a, 0x05, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x22, 0x97, 0x03,
	0x0a, 0x04, 0x43, 0x6f, 0x6e, 0x66, 0x12, 0x18, 0x0a, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f,
	0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e,
	0x12, 0x1a, 0x0a, 0x08, 0x73, 0x68, 0x61, 0x72, 0x64, 0x4e, 0x75, 0x6d, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x05, 0x52, 0x08, 0x73, 0x68, 0x61, 0x72, 0x64, 0x4e, 0x75, 0x6d, 0x12, 0x36, 0x0a, 0x08,
	0x69, 0x64, 0x32, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1a,
	0x2e, 0x6d, 0x61, 0x73, 0x74, 0x65, 0x72, 0x2e, 0x43, 0x6f, 0x6e, 0x66, 0x2e, 0x49, 0x64, 0x32,
	0x67, 0x72, 0x6f, 0x75, 0x70, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x08, 0x69, 0x64, 0x32, 0x67,
	0x72, 0x6f, 0x75, 0x70, 0x12, 0x3c, 0x0a, 0x0a, 0x61, 0x73, 0x73, 0x69, 0x67, 0x6e, 0x6d, 0x65,
	0x6e, 0x74, 0x18, 0x04, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1c, 0x2e, 0x6d, 0x61, 0x73, 0x74, 0x65,
	0x72, 0x2e, 0x43, 0x6f, 0x6e, 0x66, 0x2e, 0x41, 0x73, 0x73, 0x69, 0x67, 0x6e, 0x6d, 0x65, 0x6e,
	0x74, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x0a, 0x61, 0x73, 0x73, 0x69, 0x67, 0x6e, 0x6d, 0x65,
	0x6e, 0x74, 0x1a, 0x53, 0x0a, 0x05, 0x47, 0x72, 0x6f, 0x75, 0x70, 0x12, 0x18, 0x0a, 0x07, 0x67,
	0x72, 0x6f, 0x75, 0x70, 0x49, 0x44, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x07, 0x67, 0x72,
	0x6f, 0x75, 0x70, 0x49, 0x44, 0x12, 0x18, 0x0a, 0x07, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x73,
	0x18, 0x02, 0x20, 0x03, 0x28, 0x09, 0x52, 0x07, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x73, 0x12,
	0x16, 0x0a, 0x06, 0x73, 0x68, 0x61, 0x72, 0x64, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x05, 0x52,
	0x06, 0x73, 0x68, 0x61, 0x72, 0x64, 0x73, 0x1a, 0x4f, 0x0a, 0x0d, 0x49, 0x64, 0x32, 0x67, 0x72,
	0x6f, 0x75, 0x70, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x28, 0x0a, 0x05, 0x76, 0x61,
	0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x12, 0x2e, 0x6d, 0x61, 0x73, 0x74,
	0x65, 0x72, 0x2e, 0x43, 0x6f, 0x6e, 0x66, 0x2e, 0x47, 0x72, 0x6f, 0x75, 0x70, 0x52, 0x05, 0x76,
	0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x1a, 0x3d, 0x0a, 0x0f, 0x41, 0x73, 0x73, 0x69,
	0x67, 0x6e, 0x6d, 0x65, 0x6e, 0x74, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b,
	0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a,
	0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x05, 0x76, 0x61,
	0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x32, 0x98, 0x01, 0x0a, 0x0f, 0x53, 0x68, 0x61, 0x72,
	0x64, 0x69, 0x6e, 0x67, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x2a, 0x0a, 0x04, 0x4a,
	0x6f, 0x69, 0x6e, 0x12, 0x13, 0x2e, 0x6d, 0x61, 0x73, 0x74, 0x65, 0x72, 0x2e, 0x4a, 0x6f, 0x69,
	0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x0d, 0x2e, 0x6d, 0x61, 0x73, 0x74, 0x65,
	0x72, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x12, 0x2c, 0x0a, 0x05, 0x4c, 0x65, 0x61, 0x76, 0x65,
	0x12, 0x14, 0x2e, 0x6d, 0x61, 0x73, 0x74, 0x65, 0x72, 0x2e, 0x4c, 0x65, 0x61, 0x76, 0x65, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x0d, 0x2e, 0x6d, 0x61, 0x73, 0x74, 0x65, 0x72, 0x2e,
	0x45, 0x6d, 0x70, 0x74, 0x79, 0x12, 0x2b, 0x0a, 0x05, 0x51, 0x75, 0x65, 0x72, 0x79, 0x12, 0x14,
	0x2e, 0x6d, 0x61, 0x73, 0x74, 0x65, 0x72, 0x2e, 0x51, 0x75, 0x65, 0x72, 0x79, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x1a, 0x0c, 0x2e, 0x6d, 0x61, 0x73, 0x74, 0x65, 0x72, 0x2e, 0x43, 0x6f,
	0x6e, 0x66, 0x42, 0x0e, 0x5a, 0x0c, 0x64, 0x73, 0x2f, 0x67, 0x6f, 0x2f, 0x6d, 0x61, 0x73, 0x74,
	0x65, 0x72, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_go_master_master_proto_rawDescOnce sync.Once
	file_go_master_master_proto_rawDescData = file_go_master_master_proto_rawDesc
)

func file_go_master_master_proto_rawDescGZIP() []byte {
	file_go_master_master_proto_rawDescOnce.Do(func() {
		file_go_master_master_proto_rawDescData = protoimpl.X.CompressGZIP(file_go_master_master_proto_rawDescData)
	})
	return file_go_master_master_proto_rawDescData
}

var file_go_master_master_proto_msgTypes = make([]protoimpl.MessageInfo, 10)
var file_go_master_master_proto_goTypes = []interface{}{
	(*JoinRequest)(nil),             // 0: master.JoinRequest
	(*LeaveRequest)(nil),            // 1: master.LeaveRequest
	(*QueryRequest)(nil),            // 2: master.QueryRequest
	(*Empty)(nil),                   // 3: master.Empty
	(*Conf)(nil),                    // 4: master.Conf
	(*JoinRequest_ServerConfs)(nil), // 5: master.JoinRequest.ServerConfs
	nil,                             // 6: master.JoinRequest.NewGroupsEntry
	(*Conf_Group)(nil),              // 7: master.Conf.Group
	nil,                             // 8: master.Conf.Id2groupEntry
	nil,                             // 9: master.Conf.AssignmentEntry
}
var file_go_master_master_proto_depIdxs = []int32{
	6, // 0: master.JoinRequest.newGroups:type_name -> master.JoinRequest.NewGroupsEntry
	8, // 1: master.Conf.id2group:type_name -> master.Conf.Id2groupEntry
	9, // 2: master.Conf.assignment:type_name -> master.Conf.AssignmentEntry
	5, // 3: master.JoinRequest.NewGroupsEntry.value:type_name -> master.JoinRequest.ServerConfs
	7, // 4: master.Conf.Id2groupEntry.value:type_name -> master.Conf.Group
	0, // 5: master.ShardingService.Join:input_type -> master.JoinRequest
	1, // 6: master.ShardingService.Leave:input_type -> master.LeaveRequest
	2, // 7: master.ShardingService.Query:input_type -> master.QueryRequest
	3, // 8: master.ShardingService.Join:output_type -> master.Empty
	3, // 9: master.ShardingService.Leave:output_type -> master.Empty
	4, // 10: master.ShardingService.Query:output_type -> master.Conf
	8, // [8:11] is the sub-list for method output_type
	5, // [5:8] is the sub-list for method input_type
	5, // [5:5] is the sub-list for extension type_name
	5, // [5:5] is the sub-list for extension extendee
	0, // [0:5] is the sub-list for field type_name
}

func init() { file_go_master_master_proto_init() }
func file_go_master_master_proto_init() {
	if File_go_master_master_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_go_master_master_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*JoinRequest); i {
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
		file_go_master_master_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*LeaveRequest); i {
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
		file_go_master_master_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*QueryRequest); i {
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
		file_go_master_master_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Empty); i {
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
		file_go_master_master_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Conf); i {
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
		file_go_master_master_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*JoinRequest_ServerConfs); i {
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
		file_go_master_master_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Conf_Group); i {
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
			RawDescriptor: file_go_master_master_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   10,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_go_master_master_proto_goTypes,
		DependencyIndexes: file_go_master_master_proto_depIdxs,
		MessageInfos:      file_go_master_master_proto_msgTypes,
	}.Build()
	File_go_master_master_proto = out.File
	file_go_master_master_proto_rawDesc = nil
	file_go_master_master_proto_goTypes = nil
	file_go_master_master_proto_depIdxs = nil
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConnInterface

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion6

// ShardingServiceClient is the client API for ShardingService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type ShardingServiceClient interface {
	Join(ctx context.Context, in *JoinRequest, opts ...grpc.CallOption) (*Empty, error)
	Leave(ctx context.Context, in *LeaveRequest, opts ...grpc.CallOption) (*Empty, error)
	Query(ctx context.Context, in *QueryRequest, opts ...grpc.CallOption) (*Conf, error)
}

type shardingServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewShardingServiceClient(cc grpc.ClientConnInterface) ShardingServiceClient {
	return &shardingServiceClient{cc}
}

func (c *shardingServiceClient) Join(ctx context.Context, in *JoinRequest, opts ...grpc.CallOption) (*Empty, error) {
	out := new(Empty)
	err := c.cc.Invoke(ctx, "/master.ShardingService/Join", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *shardingServiceClient) Leave(ctx context.Context, in *LeaveRequest, opts ...grpc.CallOption) (*Empty, error) {
	out := new(Empty)
	err := c.cc.Invoke(ctx, "/master.ShardingService/Leave", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *shardingServiceClient) Query(ctx context.Context, in *QueryRequest, opts ...grpc.CallOption) (*Conf, error) {
	out := new(Conf)
	err := c.cc.Invoke(ctx, "/master.ShardingService/Query", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ShardingServiceServer is the server API for ShardingService service.
type ShardingServiceServer interface {
	Join(context.Context, *JoinRequest) (*Empty, error)
	Leave(context.Context, *LeaveRequest) (*Empty, error)
	Query(context.Context, *QueryRequest) (*Conf, error)
}

// UnimplementedShardingServiceServer can be embedded to have forward compatible implementations.
type UnimplementedShardingServiceServer struct {
}

func (*UnimplementedShardingServiceServer) Join(context.Context, *JoinRequest) (*Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Join not implemented")
}
func (*UnimplementedShardingServiceServer) Leave(context.Context, *LeaveRequest) (*Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Leave not implemented")
}
func (*UnimplementedShardingServiceServer) Query(context.Context, *QueryRequest) (*Conf, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Query not implemented")
}

func RegisterShardingServiceServer(s *grpc.Server, srv ShardingServiceServer) {
	s.RegisterService(&_ShardingService_serviceDesc, srv)
}

func _ShardingService_Join_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(JoinRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ShardingServiceServer).Join(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/master.ShardingService/Join",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ShardingServiceServer).Join(ctx, req.(*JoinRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ShardingService_Leave_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LeaveRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ShardingServiceServer).Leave(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/master.ShardingService/Leave",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ShardingServiceServer).Leave(ctx, req.(*LeaveRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ShardingService_Query_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(QueryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ShardingServiceServer).Query(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/master.ShardingService/Query",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ShardingServiceServer).Query(ctx, req.(*QueryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _ShardingService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "master.ShardingService",
	HandlerType: (*ShardingServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Join",
			Handler:    _ShardingService_Join_Handler,
		},
		{
			MethodName: "Leave",
			Handler:    _ShardingService_Leave_Handler,
		},
		{
			MethodName: "Query",
			Handler:    _ShardingService_Query_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "go/master/master.proto",
}
