package brahmaputra

// importing modules

import(
	"compress/zlib"
	"compress/gzip"
	"github.com/pierrec/lz4"
	"github.com/golang/snappy"
	"bytes"
	"ByteBuffer"
	"io/ioutil"
)

var(
	lzReaderObj *lz4.Reader
	snappyReaderObj *snappy.Reader
	gzipReaderObj *gzip.Reader

	lzWriterObj *lz4.Writer
	snappyWriterObj *snappy.Writer
	gzipWriterObj *gzip.Writer
	zlibWriterObj *zlib.Writer
)

// ###############################################################################################################################################
// lz4 compression 

// read method

func lz4CompressionReadMethod(bodyBB []byte)[] byte{

	defer handlepanic()

	byteRead := bytes.NewReader(bodyBB)

	if lzReaderObj == nil{

		lzReaderObj = lz4.NewReader(byteRead)

	}else{

		lzReaderObj.Reset(byteRead)
	}

	uncompressByte, err := ioutil.ReadAll(lzReaderObj)

    if err != nil {
       return nil
    }

    return uncompressByte
}

// write method

func lz4CompressionWriteMethod(byteBuffer ByteBuffer.Buffer, compressionByte bytes.Buffer, bodyBB []byte)[] byte{

	defer handlepanic()

	// creating new writer for lz4 compression

	if lzWriterObj == nil{

		lzWriterObj = lz4.NewWriter(&compressionByte)

	}else{

		lzWriterObj.Reset(&compressionByte)
	}

	// writing to lz4 for compression

	if _, err := lzWriterObj.Write(bodyBB); err != nil {
		return nil
	}

	if err := lzWriterObj.Close(); err != nil {
		return nil
	}

	// pushing actual body

	byteBuffer.Put(compressionByte.Bytes())

	return byteBuffer.Array()
}

// ###############################################################################################################################################
// snappy compression

// read method


func snappyCompressionReadMethod(bodyBB []byte)[] byte{
	
	defer handlepanic()

	byteRead := bytes.NewReader(bodyBB)

	if snappyReaderObj == nil{

		snappyReaderObj = snappy.NewReader(byteRead)

	}else{

		snappyReaderObj.Reset(byteRead)

	}

	uncompressByte, err := ioutil.ReadAll(snappyReaderObj)

    if err != nil {
       return nil
    }

    return uncompressByte
}

// write method

func snappyCompressionWriteMethod(byteBuffer ByteBuffer.Buffer, compressionByte bytes.Buffer, bodyBB []byte)[] byte{
	
	defer handlepanic()

	// creating new writer for snappy compression

	if snappyWriterObj == nil{

		snappyWriterObj = snappy.NewWriter(&compressionByte)

	}else{

		snappyWriterObj.Reset(&compressionByte)
	}

	// writing to snappy for compression

	if _, err := snappyWriterObj.Write(bodyBB); err != nil {
		return nil
	}

	if err := snappyWriterObj.Close(); err != nil {
		return nil
	}

	// pushing actual body

	byteBuffer.Put(compressionByte.Bytes())

	return byteBuffer.Array()
}

// ###############################################################################################################################################
// gzip compression

// read method


func gzipCompressionReadMethod(bodyBB []byte)[] byte{
	
	defer handlepanic()

	var err error

	byteRead := bytes.NewReader(bodyBB)

	if gzipReaderObj == nil{

		gzipReaderObj, err = gzip.NewReader(byteRead)

	}else{

		err = gzipReaderObj.Reset(byteRead)

	}

	if err != nil{
		return nil
	}

	uncompressByte, err := ioutil.ReadAll(gzipReaderObj)

    if err != nil {
       return nil
    }

    gzipReaderObj.Close()

    return uncompressByte
}

// write method

func gzipCompressionWriteMethod(byteBuffer ByteBuffer.Buffer, compressionByte bytes.Buffer, bodyBB []byte)[] byte{
	
	defer handlepanic()

	var err error

	// creating new writer for gzip compression

	if gzipWriterObj == nil{

		gzipWriterObj, err = gzip.NewWriterLevel(&compressionByte, gzip.BestCompression)

		// error while compressing

		if err != nil{
			return nil
		}

	}else{

		gzipWriterObj.Reset(&compressionByte)

	}

	// writing to gzib for compression

	if _, err := gzipWriterObj.Write(bodyBB); err != nil {
		return nil
	}

	if err := gzipWriterObj.Close(); err != nil {
		return nil
	}

	// pushing actual body

	byteBuffer.Put(compressionByte.Bytes())

	return byteBuffer.Array()
}

// ###############################################################################################################################################
// zlib compression

// read method


func zlibCompressionReadMethod(bodyBB []byte)[] byte{
	
	defer handlepanic()

	byteRead := bytes.NewReader(bodyBB)

	zlibObj, err := zlib.NewReader(byteRead)

	if err != nil{
		return nil
	}

	uncompressByte, err := ioutil.ReadAll(zlibObj)

    if err != nil {
       return nil
    }

    zlibObj.Close()

    return uncompressByte
}

// write method

func zlibCompressionWriteMethod(byteBuffer ByteBuffer.Buffer, compressionByte bytes.Buffer, bodyBB []byte)[] byte{

	defer handlepanic()

	var err error

	// creating new writer for zlib compression

	if zlibWriterObj == nil{

		zlibWriterObj, err = zlib.NewWriterLevel(&compressionByte, zlib.BestCompression)

		// error while compressing

		if err != nil{
			return nil
		}

	}else{

		zlibWriterObj.Reset(&compressionByte)

	}

	// writing to zlib for compression

	if _, err := zlibWriterObj.Write(bodyBB); err != nil {
		return nil
	}

	if err := zlibWriterObj.Close(); err != nil {
		return nil
	}

	// pushing actual body

	byteBuffer.Put(compressionByte.Bytes())

	return byteBuffer.Array()
}
