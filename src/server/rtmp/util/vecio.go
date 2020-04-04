package util

import (
	"io"
	"net"
	"os"
	"unsafe"
)

type SysIOVec struct{
	Base   uintptr
	Length uint64
}

type IOVec struct{
	Data   [][]byte
	Length int
	index  int
}

func (iov *IOVec) Append(b []byte){

	iov.Data = append(iov.Data, b)
	iov.Length += len(b)

}

func (iov *IOVec) WriteTo(w io.Writer, n int) (written int, err error){

	for n > 0 && iov.Length > 0{

		data := iov.Data[iov.index]

		var b []byte


		if n > len(data){

			b = data

		}else{

			b = data[:n]

		}


		data = data[len(b):]

		if len(data) == 0{

			iov.index++

		}else{

			iov.Data[iov.index] = data

		}

		n -= len(b)
		iov.Length -= len(b)
		written += len(b)

		if _, err = w.Write(b); err != nil{
			return
		}

	}

	return
}

type IOVecWriter struct{
	fd          uintptr
	smallBuffer []byte
	sysIOV      []SysIOVec
}

func NewIOVecWriter(w io.Writer) (iow *IOVecWriter){
	
	var err error
	var file *os.File

	switch value := w.(type){

	case *net.TCPConn:
		{
			file, err = value.File()
			if err != nil {
				return
			}
		}

	case *os.File:
		{
			file = value
		}

	default:
		return

	}


	iow = &IOVecWriter{
		fd: file.Fd(),
	}

	return
}

func (iow *IOVecWriter) Write(data []byte) (written int, err error){

	siov := SysIOVec{
		Length: uint64(len(data)),
	}


	if siov.Length < 16 {

		siov.Base = uintptr(len(iow.smallBuffer))
		iow.smallBuffer = append(iow.smallBuffer, data...)

	}else{

		siov.Base = uintptr(unsafe.Pointer(&data[0]))

	}

	iow.sysIOV = append(iow.sysIOV, siov)

	return written, nil
}

func (iow *IOVecWriter) Flush() error {

	for i, _ := range iow.sysIOV {

		siov := &iow.sysIOV[i] 

		if siov.Base < uintptr(len(iow.smallBuffer)){

			siov.Base = uintptr(unsafe.Pointer(&iow.smallBuffer[siov.Base]))
		}

	}

	N := 1024

	count := len(iow.sysIOV)

	for i := 0; i < count; i += N{

		n := count - i
		if n > N {
			n = N
		}

	}

	iow.sysIOV = iow.sysIOV[:0]
	iow.smallBuffer = iow.smallBuffer[:0]

	return nil
}
