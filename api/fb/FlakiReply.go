// automatically generated by the FlatBuffers compiler, do not modify

package fb

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type FlakiReply struct {
	_tab flatbuffers.Table
}

func GetRootAsFlakiReply(buf []byte, offset flatbuffers.UOffsetT) *FlakiReply {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &FlakiReply{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *FlakiReply) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *FlakiReply) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *FlakiReply) Id() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func FlakiReplyStart(builder *flatbuffers.Builder) {
	builder.StartObject(1)
}
func FlakiReplyAddId(builder *flatbuffers.Builder, id flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(id), 0)
}
func FlakiReplyEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
