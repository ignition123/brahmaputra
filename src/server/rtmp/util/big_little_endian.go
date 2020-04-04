package util

var LittleEndian littleEndian
var BigEndian bigEndian

type littleEndian struct{}


func (littleEndian) Uint16(b []byte) uint16{ 

	return uint16(b[0]) | uint16(b[1])<<8

}

func (littleEndian) Uint24(b []byte) uint32{ 

	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 

}

func (littleEndian) Uint32(b []byte) uint32{

	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24

}

func (littleEndian) Uint40(b []byte) uint64{

	return uint64(b[0]) | uint64(b[1])<<8 |
		uint64(b[2])<<16 | uint64(b[3])<<24 | uint64(b[4])<<32

}

func (littleEndian) Uint48(b []byte) uint64{

	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 |
		uint64(b[3])<<24 | uint64(b[4])<<32 | uint64(b[5])<<40

}

func (littleEndian) Uint64(b []byte) uint64{

	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56

}

func (littleEndian) PutUint16(b []byte, v uint16){

	b[0] = byte(v)
	b[1] = byte(v >> 8)

}

func (littleEndian) PutUint24(b []byte, v uint32){

	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)

}

func (littleEndian) PutUint32(b []byte, v uint32){

	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)

}

func (littleEndian) PutUint64(b []byte, v uint64){

	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
	b[4] = byte(v >> 32)
	b[5] = byte(v >> 40)
	b[6] = byte(v >> 48)
	b[7] = byte(v >> 56)

}

func (littleEndian) ToUint16(v uint16) []byte{

	b := make([]byte, 2)
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	return b

}

func (littleEndian) ToUint24(v uint32) []byte{

	b := make([]byte, 3)
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	return b

}

func (littleEndian) ToUint32(v uint32) []byte{

	b := make([]byte, 4)
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
	return b

}

func (littleEndian) ToUint40(v uint64) []byte{

	b := make([]byte, 5)
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
	b[4] = byte(v >> 32)
	return b

}

func (littleEndian) ToUint48(v uint64) []byte{

	b := make([]byte, 6)
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
	b[4] = byte(v >> 32)
	b[5] = byte(v >> 40)
	return b

}

func (littleEndian) ToUint64(v uint64) []byte{

	b := make([]byte, 8)
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
	b[4] = byte(v >> 32)
	b[5] = byte(v >> 40)
	b[6] = byte(v >> 48)
	b[7] = byte(v >> 56)
	return b

}

type bigEndian struct{}

func (bigEndian) Uint16(b []byte) uint16{ 
	return uint16(b[1]) | uint16(b[0])<<8
}

func (bigEndian) Uint24(b []byte) uint32{ 
	return uint32(b[2]) | uint32(b[1])<<8 | uint32(b[0])<<16 
}

func (bigEndian) Uint32(b []byte) uint32 {

	return uint32(b[3]) | uint32(b[2])<<8 | uint32(b[1])<<16 | uint32(b[0])<<24

}

func (bigEndian) Uint40(b []byte) uint64{

	return uint64(b[4]) | uint64(b[3])<<8 |
		uint64(b[2])<<16 | uint64(b[1])<<24 | uint64(b[0])<<32

}

func (bigEndian) Uint48(b []byte) uint64{

	return uint64(b[5]) | uint64(b[4])<<8 | uint64(b[3])<<16 |
		uint64(b[2])<<24 | uint64(b[1])<<32 | uint64(b[0])<<40

}

func (bigEndian) Uint64(b []byte) uint64{

	return uint64(b[7]) | uint64(b[6])<<8 | uint64(b[5])<<16 | uint64(b[4])<<24 |
		uint64(b[3])<<32 | uint64(b[2])<<40 | uint64(b[1])<<48 | uint64(b[0])<<56

}

func (bigEndian) PutUint16(b []byte, v uint16){

	b[0] = byte(v >> 8)
	b[1] = byte(v)

}

func (bigEndian) PutUint24(b []byte, v uint32){

	b[0] = byte(v >> 16)
	b[1] = byte(v >> 8)
	b[2] = byte(v)

}

func (bigEndian) PutUint32(b []byte, v uint32){

	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)

}

func (bigEndian) PutUint64(b []byte, v uint64){

	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)

}

func (bigEndian) ToUint16(v uint16) []byte{

	b := make([]byte, 2)
	b[0] = byte(v >> 8)
	b[1] = byte(v)
	return b

}

func (bigEndian) ToUint24(v uint32) []byte{

	b := make([]byte, 3)
	b[0] = byte(v >> 16)
	b[1] = byte(v >> 8)
	b[2] = byte(v)
	return b

}

func (bigEndian) ToUint32(v uint32) []byte{

	b := make([]byte, 4)
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
	return b

}

func (bigEndian) ToUint40(v uint64) []byte{

	b := make([]byte, 5)
	b[0] = byte(v >> 32)
	b[1] = byte(v >> 24)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 8)
	b[4] = byte(v)
	return b

}

func (bigEndian) ToUint48(v uint64) []byte{

	b := make([]byte, 6)
	b[0] = byte(v >> 40)
	b[1] = byte(v >> 32)
	b[2] = byte(v >> 24)
	b[3] = byte(v >> 16)
	b[4] = byte(v >> 8)
	b[5] = byte(v)
	return b

}

func (bigEndian) ToUint64(v uint64) []byte {
	b := make([]byte, 8)
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
	return b
}


func GetUev(buff []byte, start int) (value int, pos int){

	l := len(buff)

	var nZeroNum uint = 0

	for start < l*8{

		if (buff[start/8] & (0x80 >> uint(start%8))) > 0{

			break

		}

		nZeroNum += 1
		start += 1
	}

	dwRet := 0

	start += 1

	var i uint

	for i = 0; i < nZeroNum; i++{

		dwRet <<= 1

		if (buff[start/8] & (0x80 >> uint(start%8))) > 0{

			dwRet += 1

		}

		start += 1

	}

	return (1 << nZeroNum) - 1 + dwRet, start
}

func BigLittleSwap(v uint) uint{

	return (v >> 24) | ((v>>16)&0xff)<<8 | ((v>>8)&0xff)<<16 | (v&0xff)<<24
	
}
