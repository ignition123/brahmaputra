package mpegts

import (
	"server/rtmp/rtmplog"
	"server/rtmp/util"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
)

// NALU AUD 00 00 00 01 09 F0

const (
	TS_PACKET_SIZE      = 188
	TS_DVHS_PACKET_SIZE = 192
	TS_FEC_PACKET_SIZE  = 204

	TS_MAX_PACKET_SIZE = 204

	PID_PAT        = 0x0000
	PID_CAT        = 0x0001
	PID_TSDT       = 0x0002
	PID_RESERVED1  = 0x0003
	PID_RESERVED2  = 0x000F
	PID_NIT_ST     = 0x0010
	PID_SDT_BAT_ST = 0x0011
	PID_EIT_ST     = 0x0012
	PID_RST_ST     = 0x0013
	PID_TDT_TOT_ST = 0x0014
	PID_NET_SYNC   = 0x0015
	PID_RESERVED3  = 0x0016
	PID_RESERVED4  = 0x001B
	PID_SIGNALLING = 0x001C
	PID_MEASURE    = 0x001D
	PID_DIT        = 0x001E
	PID_SIT        = 0x001F
	// 0x0003 - 0x000F Reserved
	// 0x0010 - 0x1FFE May be assigned as network_PID, Program_map_PID, elementary_PID, or for other purposes
	// 0x1FFF Null Packet

	// program_association_section
	// conditional_access_section
	// TS_program_map_section
	// TS_description_section
	// ISO_IEC_14496_scene_description_section
	// ISO_IEC_14496_object_descriptor_section
	// Metadata_section
	// IPMP_Control_Information_section (defined in ISO/IEC 13818-11)
	TABLE_PAS               = 0x00
	TABLE_CAS               = 0x01
	TABLE_TSPMS             = 0x02
	TABLE_TSDS              = 0x03
	TABLE_ISO_IEC_14496_SDC = 0x04
	TABLE_ISO_IEC_14496_ODC = 0x05
	TABLE_MS                = 0x06
	TABLE_IPMP_CIS          = 0x07
	// 0x06 - 0x37 ITU-T Rec. H.222.0 | ISO/IEC 13818-1 reserved
	// 0x38 - 0x3F Defined in ISO/IEC 13818-6
	// 0x40 - 0xFE User private
	// 0xFF Forbidden

	STREAM_TYPE_H264 = 0x1B
	STREAM_TYPE_AAC  = 0X0F

	// 1110 xxxx
	// 110x xxxx
	STREAM_ID_VIDEO = 0xE0 // ITU-T Rec. H.262 | ISO/IEC 13818-2 or ISO/IEC 11172-2 or ISO/IEC14496-2 video stream number xxxx
	STREAM_ID_AUDIO = 0xC0 // ISO/IEC 13818-3 or ISO/IEC 11172-3 or ISO/IEC 13818-7 or ISO/IEC14496-3 audio stream number x xxxx

	PAT_PKT_TYPE = 0
	PMT_PKT_TYPE = 1
	PES_PKT_TYPE = 2
)

//
// MPEGTS -> PAT + PMT + PES
// ES -> PES -> TS
//

type MpegTsStream struct{

	firstTsPkt *MpegTsPacket 
	patPkt     *MpegTsPacket 
	pmtPkt     *MpegTsPacket 
	pat        *MpegTsPAT    
	pmt        *MpegTsPMT  

	patCh        chan *MpegTsPAT      
	pmtCh        chan *MpegTsPMT     
	tsPesPktChan chan *MpegTsPesStream

}

func NewMpegTsStream() (ts *MpegTsStream){

	ts = new(MpegTsStream)

	ts.firstTsPkt = new(MpegTsPacket)

	ts.patPkt = new(MpegTsPacket)

	ts.pmtPkt = new(MpegTsPacket)

	ts.pat = new(MpegTsPAT)

	ts.pmt = new(MpegTsPMT)

	ts.patCh = make(chan *MpegTsPAT, 0)

	ts.pmtCh = make(chan *MpegTsPMT, 0)

	ts.tsPesPktChan = make(chan *MpegTsPesStream, 0)

	return

}

// ios13818-1-CN.pdf 33/165
//
// TS
//

// Packet == Header + Payload == 188 bytes

type MpegTsPacket struct{

	Header  MpegTsHeader
	Payload []byte

}

type MpegTsHeader struct{

	SyncByte                   byte   // 8 bits 
	TransportErrorIndicator    byte   // 1 bit
	PayloadUnitStartIndicator  byte   // 1 bit 
	TransportPriority          byte   // 1 bit 
	Pid                        uint16 // 13 bits
	TransportScramblingControl byte   // 2 bits 
	AdaptionFieldControl       byte   // 2 bits 
	ContinuityCounter          byte   // 4 bits

	MpegTsHeaderAdaptationField

}

type MpegTsHeaderAdaptationField struct{

	AdaptationFieldLength             byte // 8bits 
	DiscontinuityIndicator            byte // 1bit 
	RandomAccessIndicator             byte // 1bit 
	ElementaryStreamPriorityIndicator byte // 1bit 
	PCRFlag                           byte // 1bit 
	OPCRFlag                          byte // 1bit 
	SplicingPointFlag                 byte // 1bit 
	TrasportPrivateDataFlag           byte // 1bit
	AdaptationFieldExtensionFlag      byte // 1bit

	// Optional Fields
	ProgramClockReferenceBase              uint64 // 33 bits pcr
	Reserved1                              byte   // 6 bits
	ProgramClockReferenceExtension         uint16 // 9 bits
	OriginalProgramClockReferenceBase      uint64 // 33 bits opcr
	Reserved2                              byte   // 6 bits
	OriginalProgramClockReferenceExtension uint16 // 9 bits
	SpliceCountdown                        byte   // 8 bits
	TransportPrivateDataLength             byte   // 8 bits 
	PrivateDataByte                        byte   // 8 bits
	AdaptationFieldExtensionLength         byte   // 8 bits
	LtwFlag                                byte   // 1 bit 
	PiecewiseRateFlag                      byte   // 1 bit 
	SeamlessSpliceFlag                     byte   // 1 bit

	// Optional Fields
	LtwValidFlag  byte   // 1 bit 
	LtwOffset     uint16 // 15 bits 
	Reserved3     byte   // 2 bits 
	PiecewiseRate uint32 // 22 bits
	SpliceType    byte   // 4 bits
	DtsNextAU     uint64 // 33 bits

	// stuffing bytes
}

// ios13818-1-CN.pdf 77
//
// Descriptor
//

type MpegTsDescriptor struct{

	Tag    byte // 8 bits
	Length byte // 8 bits
	Data   []byte

}

func ReadTsPacket(r io.Reader) (packet MpegTsPacket, err error){

	lr := &io.LimitedReader{R: r, N: TS_PACKET_SIZE}

	// header
	packet.Header, err = ReadTsHeader(lr)

	if err != nil{

		return

	}

	// payload
	packet.Payload = make([]byte, lr.N)

	_, err = lr.Read(packet.Payload)

	if err != nil{

		return

	}

	return
}

func ReadTsHeader(r io.Reader) (header MpegTsHeader, err error){

	var h uint32

	// MPEGTS Header 4个字节
	h, err = util.ReadByteToUint32(r, true)

	if err != nil{

		return

	}

	// | 1111 1111 | 0000 0000 | 0000 0000 | 0000 0000 |
	if (h&0xff000000)>>24 != 0x47{

		err = errors.New("mpegts header sync error!")

		return

	}

	// | 1111 1111 | 0000 0000 | 0000 0000 | 0000 0000 |
	header.SyncByte = byte((h & 0xff000000) >> 24)

	// | 0000 0000 | 1000 0000 | 0000 0000 | 0000 0000 |
	header.TransportErrorIndicator = byte((h & 0x800000) >> 23)

	// | 0000 0000 | 0100 0000 | 0000 0000 | 0000 0000 |
	header.PayloadUnitStartIndicator = byte((h & 0x400000) >> 22)

	// | 0000 0000 | 0010 0000 | 0000 0000 | 0000 0000 |
	header.TransportPriority = byte((h & 0x200000) >> 21)

	// | 0000 0000 | 0001 1111 | 1111 1111 | 0000 0000 |
	header.Pid = uint16((h & 0x1fff00) >> 8)

	// | 0000 0000 | 0000 0000 | 0000 0000 | 1100 0000 |
	header.TransportScramblingControl = byte((h & 0xc0) >> 6)

	// | 0000 0000 | 0000 0000 | 0000 0000 | 0011 0000 |
	// 0x30 , 0x20 -> adaptation_field, 0x10 -> Payload
	header.AdaptionFieldControl = byte((h & 0x30) >> 4)

	// | 0000 0000 | 0000 0000 | 0000 0000 | 0000 1111 |
	header.ContinuityCounter = byte(h & 0xf)

	if header.AdaptionFieldControl >= 2{

		// adaptationFieldLength
		header.AdaptationFieldLength, err = util.ReadByteToUint8(r)

		if err != nil{

			return

		}

		if header.AdaptationFieldLength > 0{

			lr := &io.LimitedReader{R: r, N: int64(header.AdaptationFieldLength)}

			var flags uint8

			flags, err = util.ReadByteToUint8(lr)

			if err != nil{

				return

			}

			header.DiscontinuityIndicator = flags & 0x80
			header.RandomAccessIndicator = flags & 0x40
			header.ElementaryStreamPriorityIndicator = flags & 0x20
			header.PCRFlag = flags & 0x10
			header.OPCRFlag = flags & 0x08
			header.SplicingPointFlag = flags & 0x04
			header.TrasportPrivateDataFlag = flags & 0x02
			header.AdaptationFieldExtensionFlag = flags & 0x01

			if header.RandomAccessIndicator != 0 {

			}

			if header.PCRFlag != 0{

				var pcr uint64

				pcr, err = util.ReadByteToUint48(lr, true)

				if err != nil{

					return

				}

				header.ProgramClockReferenceBase = pcr >> 15                // 9 bits  + 6 bits

				header.ProgramClockReferenceExtension = uint16(pcr & 0x1ff) // 9 bits -> | 0000 0001 | 1111 1111 |

			}

			// OPCRFlag
			if header.OPCRFlag != 0{

				var opcr uint64

				opcr, err = util.ReadByteToUint48(lr, true)

				if err != nil{

					return

				}

				// OPCR(i) = OPCR_base(i)*300 + OPCR_ext(i)
				// afd.originalProgramClockReferenceBase * 300 + afd.originalProgramClockReferenceExtension
				header.OriginalProgramClockReferenceBase = opcr >> 15                // 9 bits  + 6 bits
				header.OriginalProgramClockReferenceExtension = uint16(opcr & 0x1ff) // 9 bits -> | 0000 0001 | 1111 1111 |

			}

			// splicingPointFlag
			// 1->指示 splice_countdown 字段必须在相关自适应字段中存在,指定拼接点的出现.
			// 0->指示自适应字段中 splice_countdown 字段不存在

			if header.SplicingPointFlag != 0{

				header.SpliceCountdown, err = util.ReadByteToUint8(lr)

				if err != nil{

					return

				}

			}

			// trasportPrivateDataFlag
			// 1->指示自适应字段包含一个或多个 private_data 字节.
			// 0->指示自适应字段不包含任何 private_data 字节

			if header.TrasportPrivateDataFlag != 0{

				header.TransportPrivateDataLength, err = util.ReadByteToUint8(lr)

				if err != nil{

					return

				}

				// privateDataByte

				b := make([]byte, header.TransportPrivateDataLength)

				if _, err = lr.Read(b); err != nil{

					return

				}

			}

			// adaptationFieldExtensionFlag

			if header.AdaptationFieldExtensionFlag != 0{

			}

			if lr.N > 0 {

				if _, err = io.CopyN(ioutil.Discard, lr, int64(lr.N)); err != nil{

					return

				}

			}

		}
	}

	return
}

func WriteTsHeader(w io.Writer, header MpegTsHeader) (written int, err error){

	if header.SyncByte != 0x47{

		err = errors.New("mpegts header sync error!")

		return

	}

	h := uint32(header.SyncByte)<<24 + uint32(header.TransportErrorIndicator)<<23 + uint32(header.PayloadUnitStartIndicator)<<22 + uint32(header.TransportPriority)<<21 + uint32(header.Pid)<<8 + uint32(header.TransportScramblingControl)<<6 + uint32(header.AdaptionFieldControl)<<4 + uint32(header.ContinuityCounter)
	
	if err = util.WriteUint32ToByte(w, h, true); err != nil{

		return

	}

	written += 4

	if header.AdaptionFieldControl >= 2{

		// adaptationFieldLength(8)
		if err = util.WriteUint8ToByte(w, header.AdaptationFieldLength); err != nil{

			return

		}

		written += 1

		if header.AdaptationFieldLength > 0{

			threeIndicatorFiveFlags := uint8(header.DiscontinuityIndicator<<7) + uint8(header.RandomAccessIndicator<<6) + uint8(header.ElementaryStreamPriorityIndicator<<5) + uint8(header.PCRFlag<<4) + uint8(header.OPCRFlag<<3) + uint8(header.SplicingPointFlag<<2) + uint8(header.TrasportPrivateDataFlag<<1) + uint8(header.AdaptationFieldExtensionFlag)
			
			if err = util.WriteUint8ToByte(w, threeIndicatorFiveFlags); err != nil{

				return

			}

			written += 1

			if header.PCRFlag != 0{

				pcr := header.ProgramClockReferenceBase<<15 | 0x3f<<9 | uint64(header.ProgramClockReferenceExtension)
				
				if err = util.WriteUint48ToByte(w, pcr, true); err != nil{

					return

				}

				written += 6

			}

			// OPCRFlag
			if header.OPCRFlag != 0{

				opcr := header.OriginalProgramClockReferenceBase<<15 | 0x3f<<9 | uint64(header.OriginalProgramClockReferenceExtension)
				
				if err = util.WriteUint48ToByte(w, opcr, true); err != nil{

					return

				}

				written += 6

			}

		}

	}

	return
}

func (s *MpegTsStream) TestRead(fileName string) error{

	defer func(){

		close(s.patCh)

		close(s.pmtCh)

		close(s.tsPesPktChan)

	}()

	file, err := os.Open(fileName)

	if err != nil{

		panic(err)

	}

	defer file.Close()
	var frame int64
	var tsPktArr []MpegTsPacket

	for{

		packet, err := ReadTsPacket(file)

		if err != nil{

			if err == io.EOF{

				pesPkt, err1 := TsToPES(tsPktArr)

				if err1 != nil{

					return err1

				}

				s.tsPesPktChan <- &MpegTsPesStream{
					TsPkt:  *s.firstTsPkt,
					PesPkt: pesPkt,
				}

			}else{

				rtmplog.WriteLog(err.Error())

				return err

			}

		}

		pr := bytes.NewReader(packet.Payload)

		if PID_PAT == packet.Header.Pid{

			// Header + PSI + Paylod
			pat, err := ReadPAT(pr)

			if err != nil{

				rtmplog.WriteLog(err.Error())

				return err

			}

			// send pat
			s.pat = &pat
			s.patPkt = &packet
			s.patCh <- &pat

		}

		for _, v := range s.pat.Program{

			if v.ProgramMapPID == packet.Header.Pid{

				// Header + PSI + Paylod
				pmt, err := ReadPMT(pr)

				if err != nil{

					rtmplog.WriteLog(err.Error())

					return err

				}

				// send pmt
				s.pmt = &pmt
				s.pmtPkt = &packet
				s.pmtCh <- &pmt
			}

		}

		for _, v := range s.pmt.Stream{

			if v.ElementaryPID == packet.Header.Pid{

				if packet.Header.PayloadUnitStartIndicator == 1{

					if frame != 0{

						pesPkt, err := TsToPES(tsPktArr)

						if err != nil{

							return err

						}

						s.tsPesPktChan <- &MpegTsPesStream{
							TsPkt:  *s.firstTsPkt,
							PesPkt: pesPkt,
						}

						tsPktArr = nil
					}

					s.firstTsPkt = &packet

					frame++
				}

				tsPktArr = append(tsPktArr, packet)

			}

		}

	}

	return nil
}

func (s *MpegTsStream) TestWrite(fileName string) error{

	file, err := os.Create(fileName)

	if err != nil{

		panic(err)

	}

	defer file.Close()

	pat, ok := <-s.patCh

	if !ok{

		return errors.New("recv pat error")

	}

	patTsHeader := []byte{0x47, 0x40, 0x00, 0x10}

	if err := WritePATPacket(file, patTsHeader, *pat); err != nil{

		panic(err)

	}

	pmt, ok := <-s.pmtCh

	if !ok{

		return errors.New("recv pmt error")

	}

	pmtTsHeader := []byte{0x47, 0x41, 0x00, 0x10}

	if err := WritePMTPacket(file, pmtTsHeader, *pmt); err != nil{

		panic(err)

	}

	var videoFrame int
	var audioFrame int

	for{

		tsPesPkt, ok := <-s.tsPesPktChan

		if !ok{

			fmt.Println("frame index, video , audio :", videoFrame, audioFrame)
			break

		}

		if tsPesPkt.PesPkt.Header.StreamID == STREAM_ID_AUDIO{

			audioFrame++

		}

		if tsPesPkt.PesPkt.Header.StreamID == STREAM_ID_VIDEO{

			videoFrame++

		}


		fmt.Sprintf("%s", tsPesPkt)

	}

	return nil
}

func (s *MpegTsStream) Test() error{

	go s.TestRead("in.ts")

	err := s.TestWrite("out.ts")

	if err != nil{

		return err
		
	}

	return nil
}
