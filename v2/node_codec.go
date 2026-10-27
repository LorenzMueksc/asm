package asm

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"

	// registers the proto codec the vtCodec falls back to
	_ "google.golang.org/grpc/encoding/proto"
)

// The messages of the node's protos are generated with vtprotobuf too (service/*_vtproto.pb.go): their generated
// code marshals and unmarshals without reflection. The raft entries and snapshots use it directly, the gRPC server of
// the node port and the node's connections through vtCodec. The wire format is protobuf either way, so other
// clients (the dev tool, the public listener's callers) aren't affected. Entities and payloads are user types in Any
// fields and keep proto.Marshal.

// vtMessage is a message generated with vtprotobuf.
type vtMessage interface {
	SizeVT() int
	MarshalToSizedBufferVT([]byte) (int, error)
	UnmarshalVT([]byte) error
}

// vtCodec is a gRPC codec using the vtprotobuf code of a message, proto for other messages.
type vtCodec struct{}

func (vtCodec) Name() string { return "proto" }

func (vtCodec) Marshal(v any) (mem.BufferSlice, error) {
	m, ok := v.(vtMessage)
	if !ok {
		return encoding.GetCodecV2("proto").Marshal(v)
	}
	size := m.SizeVT()
	if mem.IsBelowBufferPoolingThreshold(size) {
		buf := make([]byte, size)
		if _, err := m.MarshalToSizedBufferVT(buf); err != nil {
			return nil, err
		}
		return mem.BufferSlice{mem.SliceBuffer(buf)}, nil
	}
	pool := mem.DefaultBufferPool()
	buf := pool.Get(size)
	if _, err := m.MarshalToSizedBufferVT(*buf); err != nil {
		pool.Put(buf)
		return nil, err
	}
	return mem.BufferSlice{mem.NewBuffer(buf, pool)}, nil
}

func (vtCodec) Unmarshal(data mem.BufferSlice, v any) error {
	m, ok := v.(vtMessage)
	if !ok {
		return encoding.GetCodecV2("proto").Unmarshal(data, v)
	}
	// the generated code copies strings and bytes, the buffer goes back to the pool afterwards
	buf := data.MaterializeToBuffer(mem.DefaultBufferPool())
	defer buf.Free()
	return m.UnmarshalVT(buf.ReadOnlyData())
}

// codecServerOption and codecDialOption set the vtCodec on the node port and the node's connections.
func codecServerOption() grpc.ServerOption {
	return grpc.ForceServerCodecV2(vtCodec{})
}

func codecDialOption() grpc.DialOption {
	return grpc.WithDefaultCallOptions(grpc.ForceCodecV2(vtCodec{}))
}
