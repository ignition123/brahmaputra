package avformat

import (
	"io"
)

// Start Code + NAL Unit -> NALU Header + NALU Body
// RTP Packet -> NALU Header + NALU Body

// NALU Body -> Slice Header + Slice data
// Slice data -> flags + Macroblock layer1 + Macroblock layer2 + ...
// Macroblock layer1 -> mb_type + PCM Data
// Macroblock layer2 -> mb_type + Sub_mb_pred or mb_pred + Residual Data
// Residual Data ->

const (
	// NALU Type
	NALU_Unspecified           = 0
	NALU_Non_IDR_Picture       = 1
	NALU_Data_Partition_A      = 2
	NALU_Data_Partition_B      = 3
	NALU_Data_Partition_C      = 4
	NALU_IDR_Picture           = 5
	NALU_SEI                   = 6
	NALU_SPS                   = 7
	NALU_PPS                   = 8
	NALU_Access_Unit_Delimiter = 9
	NALU_Sequence_End          = 10
	NALU_Stream_End            = 11
	NALU_Filler_Data           = 12
	NALU_SPS_Extension         = 13
	NALU_Prefix                = 14
	NALU_SPS_Subset            = 15
	NALU_DPS                   = 16
	NALU_Reserved1             = 17
	NALU_Reserved2             = 18
	NALU_Not_Auxiliary_Coded   = 19
	NALU_Coded_Slice_Extension = 20
	NALU_Reserved3             = 21
	NALU_Reserved4             = 22
	NALU_Reserved5             = 23
	NALU_NotReserved           = 24
	// 24 - 31 NALU_NotReserved
)

var NALU_AUD_BYTE = []byte{0x00, 0x00, 0x00, 0x01, 0x09, 0xF0}
var NALU_Delimiter1 = []byte{0x00, 0x00, 0x01}
var NALU_Delimiter2 = []byte{0x00, 0x00, 0x00, 0x01}

var NALU_SEI_BYTE []byte

// H.264/AVC
// NAL - Network Abstract Layer
// raw byte sequence payload (RBSP) 原始字节序列载荷

type H264 struct{

}

type NALUnit struct{

	NALUHeader
	RBSP

}

type NALUHeader struct{

	forbidden_zero_bit byte // 1 bit  0
	nal_ref_idc        byte // 2 bits nal_unit_type等于6,9,10,
	nal_uint_type      byte // 5 bits 包含在 NAL 单元中的 RBSP

}

type RBSP interface{

}

func ReadPPS(w io.Writer){

}
