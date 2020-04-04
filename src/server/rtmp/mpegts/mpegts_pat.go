package mpegts

import (
	"server/rtmp/util"
	"bytes"
	"errors"
	"fmt"
	"io"
)

// ios13818-1-CN.pdf 43(57)/166
//
// PAT
//

var DefaultPATPacket = []byte{
	// TS Header
	0x47, 0x40, 0x00, 0x10,

	// Pointer Field
	0x00,

	// PSI
	0x00, 0xb0, 0x0d, 0x00, 0x01, 0xc1, 0x00, 0x00,

	// PAT
	0x00, 0x01, 0xe1, 0x00,

	// CRC
	0xe8, 0xf9, 0x5e, 0x7d,

	// Stuffing 167 bytes
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
}

// TS Header :
// SyncByte = 0x47
// TransportErrorIndicator = 0(B:0), PayloadUnitStartIndicator = 1(B:0), TransportPriority = 0(B:0),
// Pid = 0,
// TransportScramblingControl = 0(B:00), AdaptionFieldControl = 1(B:01), ContinuityCounter = 0(B:0000),

// PSI :
// TableID = 0x00,
// SectionSyntaxIndicator = 1(B:1), Zero = 0(B:0), Reserved1 = 3(B:11),
// SectionLength = 13(0x00d)
// TransportStreamID = 0x0001
// Reserved2 = 3(B:11), VersionNumber = (B:00000), CurrentNextIndicator = 1(B:0),
// SectionNumber = 0x00
// LastSectionNumber = 0x00

// PAT :
// ProgramNumber = 0x0001
// Reserved3 = 15(B:1110), ProgramMapPID = 4097(0x1001)

type MpegTsPATProgram struct{

	ProgramNumber uint16 // 16 bit
	Reserved3     byte   // 3 bits 
	NetworkPID    uint16 // 13 bits 
	ProgramMapPID uint16 // 13 bit 

}

// Program Association Table
// NIT,PID=0x001f
// PMT,PID=0x100

type MpegTsPAT struct{

	// PSI
	TableID                byte   // 8 bits
	SectionSyntaxIndicator byte   // 1 bit  
	Zero                   byte   // 1 bit  
	Reserved1              byte   // 2 bits
	SectionLength          uint16 // 12 bits 
	TransportStreamID      uint16 // 16 bits 
	Reserved2              byte   // 2 bits  
	VersionNumber          byte   // 5 bits 
	CurrentNextIndicator   byte   // 1 bit  
	SectionNumber          byte   // 8 bits  
	LastSectionNumber      byte   // 8 bits 
	// N Loop
	Program []MpegTsPATProgram // PAT
	Crc32 uint32 // 32 bits

}

func ReadPAT(r io.Reader) (pat MpegTsPAT, err error){

	lr, cr, psi, err := ReadPSI(r, PSI_TYPE_PAT)

	if err != nil{

		return

	}

	pat = psi.Pat

	// N Loop

	for lr.N > 0{

		programs := MpegTsPATProgram{}

		programs.ProgramNumber, err = util.ReadByteToUint16(lr, true)

		if err != nil{

			return

		}

		if programs.ProgramNumber == 0{

			programs.NetworkPID, err = util.ReadByteToUint16(lr, true)

			if err != nil{

				return

			}

			programs.NetworkPID = programs.NetworkPID & 0x1fff

		}else{

			programs.ProgramMapPID, err = util.ReadByteToUint16(lr, true)

			if err != nil{

				return

			}

			programs.ProgramMapPID = programs.ProgramMapPID & 0x1fff

		}

		pat.Program = append(pat.Program, programs)
	}

	err = cr.ReadCrc32UIntAndCheck()

	if err != nil{

		return

	}

	return

}

func WritePAT(w io.Writer, pat MpegTsPAT) (err error){

	bw := &bytes.Buffer{}

	for _, pats := range pat.Program{

		if err = util.WriteUint16ToByte(bw, pats.ProgramNumber, true); err != nil{

			return

		}

		if pats.ProgramNumber == 0{

			if err = util.WriteUint16ToByte(bw, pats.NetworkPID&0x1fff|7<<13, true); err != nil{

				return

			}

		}else{

			// | 0001 1111 | 1111 1111 |
			// 7 << 13 -> 1110 0000 0000 0000
			if err = util.WriteUint16ToByte(bw, pats.ProgramMapPID&0x1fff|7<<13, true); err != nil{

				return

			}

		}

	}

	if pat.SectionLength == 0{

		pat.SectionLength = 2 + 3 + 4 + uint16(len(bw.Bytes()))

	}

	psi := MpegTsPSI{}

	psi.Pat = pat

	if err = WritePSI(w, PSI_TYPE_PAT, psi, bw.Bytes()); err != nil{

		return

	}

	return
}

func WritePATPacket(w io.Writer, tsHeader []byte, pat MpegTsPAT) (err error){

	if pat.TableID != TABLE_PAS{

		err = errors.New("PAT table ID error")

		return

	}

	bw := &bytes.Buffer{}

	if err = WritePAT(bw, pat); err != nil{

		return

	}

	// TODO:

	stuffingBytes := util.GetFillBytes(0xff, TS_PACKET_SIZE-4-bw.Len())

	// PATPacket = TsHeader + PAT + Stuffing Bytes

	var PATPacket []byte
	PATPacket = append(PATPacket, tsHeader...)
	PATPacket = append(PATPacket, bw.Bytes()...)
	PATPacket = append(PATPacket, stuffingBytes...)

	fmt.Println("-------------------------")
	fmt.Println("Write PAT :", PATPacket)
	fmt.Println("-------------------------")

	if _, err = w.Write(PATPacket); err != nil{

		return

	}

	return
}

func WriteDefaultPATPacket(w io.Writer) (err error){

	_, err = w.Write(DefaultPATPacket)

	if err != nil{

		return
		
	}

	return
}
