package mpegts

import (
	"server/rtmp/util"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
)

const (
	PSI_TYPE_PAT      = 1
	PSI_TYPE_PMT      = 2
	PSI_TYPE_NIT      = 3
	PSI_TYPE_CAT      = 4
	PSI_TYPE_TST      = 5
	PSI_TYPE_IPMP_CIT = 6
)

type MpegTsPSI struct{

	// PAT
	// PMT
	// CAT
	// NIT
	Pat MpegTsPAT
	Pmt MpegTsPMT

}

func ReadPSI(r io.Reader, pt uint32) (lr *io.LimitedReader, cr *util.Crc32Reader, psi MpegTsPSI, err error){

	// pointer field(8)
	pointer_field, err := util.ReadByteToUint8(r)

	if err != nil{

		return

	}

	if pointer_field != 0{

		if _, err = io.CopyN(ioutil.Discard, r, int64(pointer_field)); err != nil{

			return

		}

	}

	cr = &util.Crc32Reader{R: r, Crc32: 0xffffffff}

	tableId, err := util.ReadByteToUint8(cr)

	if err != nil{

		return

	}

	sectionSyntaxIndicatorAndSectionLength, err := util.ReadByteToUint16(cr, true)

	if err != nil{

		return

	}

	lr = &io.LimitedReader{R: cr, N: int64(sectionSyntaxIndicatorAndSectionLength & 0x3FF)}

	// PAT TransportStreamID(16) or PMT ProgramNumber(16)
	transportStreamIdOrProgramNumber, err := util.ReadByteToUint16(lr, true)

	if err != nil{

		return

	}

	// reserved2(2) + versionNumber(5) + currentNextIndicator(1)
	versionNumberAndCurrentNextIndicator, err := util.ReadByteToUint8(lr)

	if err != nil{

		return

	}

	// sectionNumber(8)
	sectionNumber, err := util.ReadByteToUint8(lr)

	if err != nil{

		return

	}

	// lastSectionNumber(8)
	lastSectionNumber, err := util.ReadByteToUint8(lr)

	if err != nil{

		return

	}

	lr.N -= 4

	switch pt{

	case PSI_TYPE_PAT:
		{
			if tableId != TABLE_PAS{

				err = errors.New(fmt.Sprintf("%s, id=%d", "read pmt table id != 2", tableId))

				return

			}

			psi.Pat.TableID = tableId
			psi.Pat.SectionSyntaxIndicator = uint8((sectionSyntaxIndicatorAndSectionLength & 0x8000) >> 15)
			psi.Pat.SectionLength = sectionSyntaxIndicatorAndSectionLength & 0x3FF
			psi.Pat.TransportStreamID = transportStreamIdOrProgramNumber
			psi.Pat.VersionNumber = versionNumberAndCurrentNextIndicator & 0x3e
			psi.Pat.CurrentNextIndicator = versionNumberAndCurrentNextIndicator & 0x01
			psi.Pat.SectionNumber = sectionNumber
			psi.Pat.LastSectionNumber = lastSectionNumber

		}
	case PSI_TYPE_PMT:
		{

			if tableId != TABLE_TSPMS{

				err = errors.New(fmt.Sprintf("%s, id=%d", "read pmt table id != 2", tableId))

				return

			}

			psi.Pmt.TableID = tableId
			psi.Pmt.SectionSyntaxIndicator = uint8((sectionSyntaxIndicatorAndSectionLength & 0x8000) >> 15)
			psi.Pmt.SectionLength = sectionSyntaxIndicatorAndSectionLength & 0x3FF
			psi.Pmt.ProgramNumber = transportStreamIdOrProgramNumber
			psi.Pmt.VersionNumber = versionNumberAndCurrentNextIndicator & 0x3e
			psi.Pmt.CurrentNextIndicator = versionNumberAndCurrentNextIndicator & 0x01
			psi.Pmt.SectionNumber = sectionNumber
			psi.Pmt.LastSectionNumber = lastSectionNumber
		}

	}

	return
}

func WritePSI(w io.Writer, pt uint32, psi MpegTsPSI, data []byte) (err error){

	var tableId, versionNumberAndCurrentNextIndicator, sectionNumber, lastSectionNumber uint8

	var sectionSyntaxIndicatorAndSectionLength, transportStreamIdOrProgramNumber uint16


	switch pt{

	case PSI_TYPE_PAT:
		{
			if psi.Pat.TableID != TABLE_PAS{

				err = errors.New(fmt.Sprintf("%s, id=%d", "write pmt table id != 0", tableId))

				return

			}

			tableId = psi.Pat.TableID
			sectionSyntaxIndicatorAndSectionLength = uint16(psi.Pat.SectionSyntaxIndicator)<<15 | 3<<12 | psi.Pat.SectionLength
			transportStreamIdOrProgramNumber = psi.Pat.TransportStreamID
			versionNumberAndCurrentNextIndicator = psi.Pat.VersionNumber<<1 | psi.Pat.CurrentNextIndicator
			sectionNumber = psi.Pat.SectionNumber
			lastSectionNumber = psi.Pat.LastSectionNumber

		}
	case PSI_TYPE_PMT:
		{
		
			if psi.Pmt.TableID != TABLE_TSPMS{

				err = errors.New(fmt.Sprintf("%s, id=%d", "write pmt table id != 2", tableId))

				return

			}

			tableId = psi.Pmt.TableID
			sectionSyntaxIndicatorAndSectionLength = uint16(psi.Pmt.SectionSyntaxIndicator)<<15 | 3<<12 | psi.Pmt.SectionLength
			transportStreamIdOrProgramNumber = psi.Pmt.ProgramNumber
			versionNumberAndCurrentNextIndicator = psi.Pmt.VersionNumber<<1 | psi.Pmt.CurrentNextIndicator
			sectionNumber = psi.Pmt.SectionNumber
			lastSectionNumber = psi.Pmt.LastSectionNumber

		}

	}

	// pointer field(8)
	if err = util.WriteUint8ToByte(w, 0); err != nil{

		return

	}

	cw := &util.Crc32Writer{W: w, Crc32: 0xffffffff}

	// table id(8)
	if err = util.WriteUint8ToByte(cw, tableId); err != nil{

		return

	}

	if err = util.WriteUint16ToByte(cw, sectionSyntaxIndicatorAndSectionLength, true); err != nil{

		return

	}

	// PAT TransportStreamID(16) or PMT ProgramNumber(16)
	if err = util.WriteUint16ToByte(cw, transportStreamIdOrProgramNumber, true); err != nil{

		return

	}

	// reserved2(2) + versionNumber(5) + currentNextIndicator(1)
	// 0x3 << 6 -> 1100 0000
	// 0x3 << 6  | 1 -> 1100 0001
	if err = util.WriteUint8ToByte(cw, versionNumberAndCurrentNextIndicator); err != nil{

		return

	}

	// sectionNumber(8)
	if err = util.WriteUint8ToByte(cw, sectionNumber); err != nil{

		return

	}

	// lastSectionNumber(8)
	if err = util.WriteUint8ToByte(cw, lastSectionNumber); err != nil{

		return

	}

	// data
	if _, err = cw.Write(data); err != nil{

		return

	}

	// crc32
	crc32 := util.BigLittleSwap(uint(cw.Crc32))

	if err = util.WriteUint32ToByte(cw, uint32(crc32), true); err != nil{

		return
		
	}

	return
}
