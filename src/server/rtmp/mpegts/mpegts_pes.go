package mpegts

import (
	"server/rtmp/avformat"
	"server/rtmp/util"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
)

// ios13818-1-CN.pdf 45/166
//
// PES
//

type MpegTsPesStream struct{

	TsPkt  MpegTsPacket
	PesPkt MpegTsPESPacket

}

type MpegTsPESPacket struct{

	Header  MpegTsPESHeader
	Payload []byte

}

type MpegTsPESHeader struct{

	PacketStartCodePrefix uint32
	StreamID              byte   
	PesPacketLength       uint16 
	MpegTsOptionalPESHeader
	PayloadLength uint64

}


type MpegTsOptionalPESHeader struct{

	ConstTen               byte // 2 bits
	PesScramblingControl   byte // 2 bits
	PesPriority            byte // 1 bit 
	DataAlignmentIndicator byte // 1 bit 
	Copyright              byte // 1 bit
	OriginalOrCopy         byte // 1 bit
	PtsDtsFlags            byte // 2 bits 
	EscrFlag               byte // 1 bit 
	EsRateFlag             byte // 1 bit
	DsmTrickModeFlag       byte // 1 bit
	AdditionalCopyInfoFlag byte // 1 bit 
	PesCRCFlag             byte // 1 bit
	PesExtensionFlag       byte // 1 bit
	PesHeaderDataLength    byte // 8 bits

	// Optional Field
	Pts                  uint64 // 33 bits
	Dts                  uint64 // 33 bits
	EscrBase             uint64 // 33 bits
	EscrExtension        uint16 // 9 bits
	EsRate               uint32 // 22 bits
	TrickModeControl     byte   // 3 bits 
	TrickModeValue       byte   // 5 bits
	AdditionalCopyInfo   byte   // 7 bits
	PreviousPESPacketCRC uint16 // 16 bits

	// PES Extension
	PesPrivateDataFlag               byte // 1 bit
	PackHeaderFieldFlag              byte // 1 bit 
	ProgramPacketSequenceCounterFlag byte // 1 bit 
	PSTDBufferFlag                   byte // 1 bit 
	Reserved                         byte // 3 bits
	PesExtensionFlag2                byte // 1 bits

	// Optional Field
	PesPrivateData               [16]byte // 128 bits 
	PackHeaderField              byte     // 8 bits
	ProgramPacketSequenceCounter byte     // 7 bits
	Mpeg1Mpeg2Identifier         byte     // 1 bit 
	OriginalStuffLength          byte     // 6 bits
	PSTDBufferScale              byte     // 1bit 
	PSTDBufferSize               uint16   // 13 bits 
	PesExtensionFieldLength      byte     // 7 bits
	StreamIDExtensionFlag        byte     // 1 bits
}

type MpegtsPESFrame struct{

	Pid                       uint16
	IsKeyFrame                bool
	ContinuityCounter         byte
	ProgramClockReferenceBase uint64

}

func ReadPESHeader(r io.Reader) (header MpegTsPESHeader, err error){

	var flags uint8
	var length uint

	// packetStartCodePrefix(24) (0x000001)
	header.PacketStartCodePrefix, err = util.ReadByteToUint24(r, true)

	if err != nil{

		return

	}

	if header.PacketStartCodePrefix != 0x0000001{

		err = errors.New("read PacketStartCodePrefix is not 0x0000001")

		return

	}

	// streamID(8)
	header.StreamID, err = util.ReadByteToUint8(r)

	if err != nil{

		return

	}

	// pes_PacketLength(16)
	header.PesPacketLength, err = util.ReadByteToUint16(r, true)

	if err != nil{

		return

	}

	length = uint(header.PesPacketLength)

	if length == 0{

		length = 1 << 31

	}

	lrPacket := &io.LimitedReader{R: r, N: int64(length)}

	lrHeader := lrPacket

	// constTen(2)
	// pes_ScramblingControl(2)
	// pes_Priority(1)
	// dataAlignmentIndicator(1)
	// copyright(1)
	// originalOrCopy(1)
	flags, err = util.ReadByteToUint8(lrHeader)

	if err != nil{

		return

	}

	header.ConstTen = flags & 0xc0
	header.PesScramblingControl = flags & 0x30
	header.PesPriority = flags & 0x08
	header.DataAlignmentIndicator = flags & 0x04
	header.Copyright = flags & 0x02
	header.OriginalOrCopy = flags & 0x01

	// pts_dts_Flags(2)
	// escr_Flag(1)
	// es_RateFlag(1)
	// dsm_TrickModeFlag(1)
	// additionalCopyInfoFlag(1)
	// pes_CRCFlag(1)
	// pes_ExtensionFlag(1)

	flags, err = util.ReadByteToUint8(lrHeader)

	if err != nil{

		return

	}

	header.PtsDtsFlags = flags & 0xc0
	header.EscrFlag = flags & 0x20
	header.EsRateFlag = flags & 0x10
	header.DsmTrickModeFlag = flags & 0x08
	header.AdditionalCopyInfoFlag = flags & 0x04
	header.PesCRCFlag = flags & 0x02
	header.PesExtensionFlag = flags & 0x01

	// pes_HeaderDataLength(8)
	header.PesHeaderDataLength, err = util.ReadByteToUint8(lrHeader)

	if err != nil{

		return

	}

	length = uint(header.PesHeaderDataLength)

	lrHeader = &io.LimitedReader{R: lrHeader, N: int64(length)}

	// PTS(33)
	if flags&0x80 != 0{

		var pts uint64

		pts, err = util.ReadByteToUint40(lrHeader, true)

		if err != nil{

			return

		}

		header.Pts = util.GetPtsDts(pts)

	}

	// DTS(33)
	if flags&0x80 != 0 && flags&0x40 != 0{

		var dts uint64

		dts, err = util.ReadByteToUint40(lrHeader, true)

		if err != nil{

			return

		}

		header.Dts = util.GetPtsDts(dts)
	}

	// reserved(2) + escr_Base1(3) + marker_bit(1) +
	// escr_Base2(15) + marker_bit(1) + escr_Base23(15) +
	// marker_bit(1) + escr_Extension(9) + marker_bit(1)
	if header.EscrFlag != 0{

		_, err = util.ReadByteToUint48(lrHeader, true)

		if err != nil{

			return

		}

		//s.pes.escr_Base = escrBaseEx & 0x3fffffffe00
		//s.pes.escr_Extension = uint16(escrBaseEx & 0x1ff)
	}

	// es_Rate(22)
	if header.EsRateFlag != 0{

		header.EsRate, err = util.ReadByteToUint24(lrHeader, true)

		if err != nil{

			return

		}

	}
	/*
		// trickModeControl(3) + trickModeValue(5)
		if s.pes.dsm_TrickModeFlag != 0 {
			trickMcMv, err := util.ReadByteToUint8(lrHeader)
			if err != nil {
				return err
			}

			s.pes.trickModeControl = trickMcMv & 0xe0
			s.pes.trickModeValue = trickMcMv & 0x1f
		}
	*/

	// marker_bit(1) + additionalCopyInfo(7)

	if header.AdditionalCopyInfoFlag != 0{

		header.AdditionalCopyInfo, err = util.ReadByteToUint8(lrHeader)

		if err != nil{

			return

		}

		header.AdditionalCopyInfo = header.AdditionalCopyInfo & 0x7f

	}

	// previous_PES_Packet_CRC(16)

	if header.PesCRCFlag != 0{

		header.PreviousPESPacketCRC, err = util.ReadByteToUint16(lrHeader, true)

		if err != nil{

			return

		}

	}

	// pes_PrivateDataFlag(1) + packHeaderFieldFlag(1) + programPacketSequenceCounterFlag(1) +
	// p_STD_BufferFlag(1) + reserved(3) + pes_ExtensionFlag2(1)

	if header.PesExtensionFlag != 0{

		var flags uint8

		flags, err = util.ReadByteToUint8(lrHeader)

		if err != nil{

			return

		}

		header.PesPrivateDataFlag = flags & 0x80
		header.PackHeaderFieldFlag = flags & 0x40
		header.ProgramPacketSequenceCounterFlag = flags & 0x20
		header.PSTDBufferFlag = flags & 0x10
		header.PesExtensionFlag2 = flags & 0x01

		// TODO:下面所有的标志位,可能获取到的数据,都简单的读取后,丢弃,如果日后需要,在这里处理

		// pes_PrivateData(128)
		if header.PesPrivateDataFlag != 0{

			if _, err = io.CopyN(ioutil.Discard, lrHeader, int64(16)); err != nil{

				return

			}

		}

		// packFieldLength(8)
		if header.PackHeaderFieldFlag != 0{

			if _, err = io.CopyN(ioutil.Discard, lrHeader, int64(1)); err != nil{

				return

			}

		}

		// marker_bit(1) + programPacketSequenceCounter(7) + marker_bit(1) +
		// mpeg1_mpeg2_Identifier(1) + originalStuffLength(6)
		if header.ProgramPacketSequenceCounterFlag != 0{

			if _, err = io.CopyN(ioutil.Discard, lrHeader, int64(2)); err != nil{

				return

			}

		}

		// 01 + p_STD_bufferScale(1) + p_STD_bufferSize(13)
		if header.PSTDBufferFlag != 0{

			if _, err = io.CopyN(ioutil.Discard, lrHeader, int64(2)); err != nil{

				return

			}

		}

		// marker_bit(1) + pes_Extension_Field_Length(7) +
		// streamIDExtensionFlag(1)
		if header.PesExtensionFlag != 0{

			if _, err = io.CopyN(ioutil.Discard, lrHeader, int64(2)); err != nil{

				return

			}

		}

	}

	// 把剩下的头的数据消耗掉

	if lrHeader.N > 0{

		if _, err = io.CopyN(ioutil.Discard, lrHeader, int64(lrHeader.N)); err != nil{

			return

		}

	}

	if lrPacket.N < 65536{

		//header.pes_PacketLength = uint16(lrPacket.N)

		header.PayloadLength = uint64(lrPacket.N)

	}

	return
}

func WritePESHeader(w io.Writer, header MpegTsPESHeader) (written int, err error){

	if header.PacketStartCodePrefix != 0x0000001{

		err = errors.New("write PacketStartCodePrefix is not 0x0000001")

		return

	}

	// packetStartCodePrefix(24) (0x000001)
	if err = util.WriteUint24ToByte(w, header.PacketStartCodePrefix, true); err != nil{

		return

	}

	written += 3

	// streamID(8)

	if err = util.WriteUint8ToByte(w, header.StreamID); err != nil{

		return

	}

	written += 1

	// pes_PacketLength(16)
	// PES包长度可能为0,这个时候,需要自己去算
	// 0 <= len <= 65535

	if err = util.WriteUint16ToByte(w, header.PesPacketLength, true); err != nil{

		return

	}

	//fmt.Println("Length :", payloadLength)
	//fmt.Println("PES Packet Length :", header.pes_PacketLength)

	written += 2

	// constTen(2)
	// pes_ScramblingControl(2)
	// pes_Priority(1)
	// dataAlignmentIndicator(1)
	// copyright(1)
	// originalOrCopy(1)
	// 1000 0001

	if header.ConstTen != 0x80{

		err = errors.New("pes header ConstTen != 0x80")

		return

	}

	flags := header.ConstTen | header.PesScramblingControl | header.PesPriority | header.DataAlignmentIndicator | header.Copyright | header.OriginalOrCopy
	
	if err = util.WriteUint8ToByte(w, flags); err != nil{

		return

	}

	written += 1

	// pts_dts_Flags(2)
	// escr_Flag(1)
	// es_RateFlag(1)
	// dsm_TrickModeFlag(1)
	// additionalCopyInfoFlag(1)
	// pes_CRCFlag(1)
	// pes_ExtensionFlag(1)
	
	sevenFlags := header.PtsDtsFlags | header.EscrFlag | header.EsRateFlag | header.DsmTrickModeFlag | header.AdditionalCopyInfoFlag | header.PesCRCFlag | header.PesExtensionFlag
	
	if err = util.WriteUint8ToByte(w, sevenFlags); err != nil{

		return

	}

	written += 1

	// pes_HeaderDataLength(8)

	if err = util.WriteUint8ToByte(w, header.PesHeaderDataLength); err != nil{

		return

	}

	written += 1

	// PtsDtsFlags == 192(11), 128(10), 64(01)禁用, 0(00)

	if header.PtsDtsFlags&0x80 != 0{

		if header.PtsDtsFlags&0x80 != 0 && header.PtsDtsFlags&0x40 != 0{

			// 11:PTS和DTS
			// PTS(33) + 4 + 3
			pts := util.PutPtsDts(header.Pts) | 3<<36

			if err = util.WriteUint40ToByte(w, pts, true); err != nil{

				return

			}

			written += 5

			// DTS(33) + 4 + 3
			dts := util.PutPtsDts(header.Dts) | 1<<36

			if err = util.WriteUint40ToByte(w, dts, true); err != nil{

				return

			}

			written += 5

		}else{
			// 10:只有PTS
			// PTS(33) + 4 + 3
			pts := util.PutPtsDts(header.Pts) | 2<<36

			if err = util.WriteUint40ToByte(w, pts, true); err != nil{

				return

			}

			written += 5

		}

	}

	return
}

func WritePESPacket(w io.Writer, frame *MpegtsPESFrame, packet MpegTsPESPacket) (err error){

	var tsPkts []byte

	if tsPkts, err = PESToTs(frame, packet); err != nil{

		return

	}

	// bw.Bytes == PES Packet

	if _, err = w.Write(tsPkts); err != nil{

		return

	}

	return

}

func IowWritePESPacket(w io.Writer, tsHeader MpegTsHeader, packet MpegTsPESPacket) (err error){

	if packet.Header.PacketStartCodePrefix != 0x000001{

		return errors.New("packetStartCodePrefix != 0x000001")

	}

	bw := &bytes.Buffer{}

	// TODO:如果头长度大于65536,字段会为0,是否要改？

	_, err = WritePESHeader(bw, packet.Header)

	if err != nil{

		return

	}

	PESPacket := &util.IOVec{}
	PESPacket.Append(bw.Bytes())     // header
	PESPacket.Append(packet.Payload) // packet

	iow := util.NewIOVecWriter(w)

	var isKeyFrame bool

	var headerLength int

	isKeyFrame = CheckPESPacketIsKeyFrame(packet)

	for i := 0; PESPacket.Length > 0; i++ {

		header := MpegTsHeader{

			SyncByte:             0x47,
			Pid:                  tsHeader.Pid,
			AdaptionFieldControl: 1,
			ContinuityCounter:    byte(i % 15),

		}

		// 每一帧开头
		if i == 0{

			header.PayloadUnitStartIndicator = 1

		}
		

		// I帧
		if isKeyFrame{

			header.AdaptionFieldControl = 0x03
			header.AdaptationFieldLength = 7
			header.PCRFlag = 1
			header.RandomAccessIndicator = tsHeader.RandomAccessIndicator
			header.ProgramClockReferenceBase = tsHeader.ProgramClockReferenceBase
			header.ProgramClockReferenceExtension = tsHeader.ProgramClockReferenceExtension

			isKeyFrame = false

		}

		// 这个包大小,会在每一次PESPacket.WriteTo中慢慢减少.
		packetLength := PESPacket.Length

		// 包不满188字节
		if packetLength < TS_PACKET_SIZE-4{

			if header.AdaptionFieldControl >= 2{

				header.AdaptationFieldLength = uint8(TS_PACKET_SIZE - 4 - 1 - packetLength - 7)

			}else{

				header.AdaptionFieldControl = 0x03
				header.AdaptationFieldLength = uint8(TS_PACKET_SIZE - 4 - 1 - packetLength)

			}

			headerLength, err = WriteTsHeader(iow, header)

			if err != nil{

				return

			}

			stuffingLength := int(header.AdaptationFieldLength - 1)

			if _, err = iow.Write(util.GetFillBytes(0xff, stuffingLength)); err != nil{

				return

			}

			headerLength += stuffingLength

		}else{

			headerLength, err = WriteTsHeader(iow, header)

			if err != nil{

				return

			}

		}

		/*
			if headerLength, err = writeTsHeader(iow, header, packetLength); err != nil {
				return
			}
		*/

		payloadLength := 188 - headerLength

		// 写PES负载
		if _, err = PESPacket.WriteTo(iow, payloadLength); err != nil{

			return

		}

	}

	iow.Flush()

	return
}

func CheckPESPacketIsKeyFrame(packet MpegTsPESPacket) bool{

	nalus := bytes.SplitN(packet.Payload, avformat.NALU_Delimiter1, -1)

	for _, v := range nalus{

		if v[0]&0x1f == avformat.NALU_IDR_Picture{

			return true

		}

	}

	return false
}

func TsToPES(tsPkts []MpegTsPacket) (pesPkt MpegTsPESPacket, err error){

	var index int

	for i := 0; i < len(tsPkts); i++{

		if tsPkts[i].Header.SyncByte != 0x47{

			err = errors.New("mpegts header sync error!")

			return

		}

		if tsPkts[i].Header.PayloadUnitStartIndicator == 1{

			index++

			if index >= 2{

				err = errors.New("TsToPES error PayloadUnitStartIndicator >= 2")

				return

			}

			r := bytes.NewReader(tsPkts[i].Payload)

			lr := &io.LimitedReader{R: r, N: int64(len(tsPkts[i].Payload))}


			// TS Packet PES Header Start Index
			hBegin := lr.N

			// PES Header
			pesPkt.Header, err = ReadPESHeader(lr)

			if err != nil{

				return

			}

			// TS Packet PES Header End Index
			hEnd := lr.N

			pesHeaderLength := hBegin - hEnd

			if pesHeaderLength > 0 && pesHeaderLength <= hBegin{

				pesPkt.Payload = append(pesPkt.Payload, tsPkts[i].Payload[pesHeaderLength:]...)

			}

		}

		if tsPkts[i].Header.PayloadUnitStartIndicator == 0{

			pesPkt.Payload = append(pesPkt.Payload, tsPkts[i].Payload...)

		}

	}

	return

}

func PESToTs(frame *MpegtsPESFrame, packet MpegTsPESPacket) (tsPkts []byte, err error){

	if packet.Header.PacketStartCodePrefix != 0x000001{

		err = errors.New("packetStartCodePrefix != 0x000001")

		return

	}

	bwPESPkt := &bytes.Buffer{}

	_, err = WritePESHeader(bwPESPkt, packet.Header)

	if err != nil{

		return

	}

	if _, err = bwPESPkt.Write(packet.Payload); err != nil{

		return

	}

	var tsHeaderLength int

	for i := 0; bwPESPkt.Len() > 0; i++{

		bwTsHeader := &bytes.Buffer{}

		tsHeader := MpegTsHeader{

			SyncByte:                  0x47,
			TransportErrorIndicator:   0,
			PayloadUnitStartIndicator: 0,
			TransportPriority:         0,
			Pid:                       frame.Pid,
			TransportScramblingControl: 0,
			AdaptionFieldControl:       1,
			ContinuityCounter:          frame.ContinuityCounter,

		}

		frame.ContinuityCounter++

		frame.ContinuityCounter = frame.ContinuityCounter % 16

		if i == 0 {

			tsHeader.PayloadUnitStartIndicator = 1

			if frame.IsKeyFrame{

				tsHeader.AdaptionFieldControl = 0x03
				tsHeader.AdaptationFieldLength = 7
				tsHeader.PCRFlag = 1
				tsHeader.RandomAccessIndicator = 1
				tsHeader.ProgramClockReferenceBase = frame.ProgramClockReferenceBase

			}

		}

		pesPktLength := bwPESPkt.Len()

		if pesPktLength < TS_PACKET_SIZE-4{

			var tsStuffingLength uint8

			tsHeader.AdaptionFieldControl = 0x03
			tsHeader.AdaptationFieldLength = uint8(TS_PACKET_SIZE - 4 - 1 - pesPktLength)

			if tsHeader.AdaptationFieldLength >= 1{

				tsStuffingLength = tsHeader.AdaptationFieldLength - 1

			}else{

				tsStuffingLength = 0

			}

			// error
			tsHeaderLength, err = WriteTsHeader(bwTsHeader, tsHeader)

			if err != nil{

				return

			}

			if tsStuffingLength > 0{

				if _, err = bwTsHeader.Write(util.GetFillBytes(0xff, int(tsStuffingLength))); err != nil{

					return

				}

			}

			tsHeaderLength += int(tsStuffingLength)

		}else{

			tsHeaderLength, err = WriteTsHeader(bwTsHeader, tsHeader)

			if err != nil{

				return

			}

		}

		tsPayloadLength := TS_PACKET_SIZE - tsHeaderLength

		tsHeaderByte := bwTsHeader.Bytes()

		tsPayloadByte := bwPESPkt.Next(tsPayloadLength)

		tsPktByte := append(tsHeaderByte, tsPayloadByte...)

		if len(tsPktByte) != TS_PACKET_SIZE{

			err = errors.New(fmt.Sprintf("%s, packet size=%d", "TS_PACKET_SIZE != 188,", len(tsPktByte)))

			return

		}

		tsPkts = append(tsPkts, tsPktByte...)
	}

	return
}
