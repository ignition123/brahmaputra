package brahmaputra

// importing modules

import(
	"compress/zlib"
	"compress/gzip"
	"github.com/pierrec/lz4"
	"github.com/golang/snappy"
	"bytes"
	"log"
	"ByteBuffer"
)

// lz4 compression 

func lz4CompressionWriteMethod(byteBuffer ByteBuffer.Buffer, compressionByte bytes.Buffer, bodyBB []byte)[] byte{

	// compression lz4 algorithm (value = 5)

	byteBuffer.PutByte(byte(lzCompression))

	// creating new writer for lz4 compression

	lzw := lz4.NewWriter(&compressionByte)

	// writing to lz4 for compression

	lzw.Write(bodyBB)

	// flushing the snappy buffer

	if err := lzw.Flush(); err != nil {
		log.Println("Failed to compress using lz4")
		return nil
	}

	// closing the gzib compression

	if err := lzw.Close(); err != nil {
		log.Println("Failed to compress using lz4")
		return nil
	}

	// pushing actual body

	byteBuffer.Put(compressionByte.Bytes())

	return byteBuffer.Array()
}

// snappy compression

func snappyCompressionWriteMethod(byteBuffer ByteBuffer.Buffer, compressionByte bytes.Buffer, bodyBB []byte)[] byte{
	// compression snappy algorithm (value = 4)

	byteBuffer.PutByte(byte(snappyCompression))

	// creating new writer for snappy compression

	snpw := snappy.NewWriter(&compressionByte)

	// writing to snappy for compression

	if _, err := snpw.Write(bodyBB); err != nil {
		log.Println("Failed to compress using snappy")
		return nil
	}

	// flushing the snappy buffer

	if err := snpw.Flush(); err != nil {
		log.Println("Failed to compress using snappy")
		return nil
	}

	// closing the snappy compression

	if err := snpw.Close(); err != nil {
		log.Println("Failed to compress using snappy")
		return nil
	}

	// pushing actual body

	byteBuffer.Put(compressionByte.Bytes())

	return byteBuffer.Array()
}

// gzip compression

func gzipCompressionWriteMethod(byteBuffer ByteBuffer.Buffer, compressionByte bytes.Buffer, bodyBB []byte)[] byte{
	// compression gzip algorithm (value = 3)

	byteBuffer.PutByte(byte(gzibCompression))

	// creating new writer for gzip compression

	gz, err := gzip.NewWriterLevel(&compressionByte, gzip.BestCompression)

	// error while compressing

	if err != nil{
		log.Println("Failed to compress using gzip")
		return nil
	}

	// writing to gzib for compression

	if _, err := gz.Write(bodyBB); err != nil {
		log.Println("Failed to compress using gzip")
		return nil
	}

	// flushing the gzip buffer

	if err := gz.Flush(); err != nil {
		log.Println("Failed to compress using gzip")
		return nil
	}

	// closing the gzib compression

	if err := gz.Close(); err != nil {
		log.Println("Failed to compress using gzip")
		return nil
	}

	// pushing actual body

	byteBuffer.Put(compressionByte.Bytes())

	return byteBuffer.Array()
}

// zlib compression

func zlibCompressionWriteMethod(byteBuffer ByteBuffer.Buffer, compressionByte bytes.Buffer, bodyBB []byte)[] byte{
	// compression zlib algorithm (value = 2)

	byteBuffer.PutByte(byte(zlibCompression))

	// creating new writer for zlib compression

	zl, err := zlib.NewWriterLevel(&compressionByte, zlib.BestCompression)

	// error while compressing

	if err != nil{
		log.Println("Failed to compress using zlib")
		return nil
	}

	// writing to zlib for compression

	if _, err := zl.Write(bodyBB); err != nil {
		log.Println("Failed to compress using zlib")
		return nil
	}

	// flushing the zlib buffer

	if err := zl.Flush(); err != nil {
		log.Println("Failed to compress using zlib")
		return nil
	}

	// closing the zlib compression

	if err := zl.Close(); err != nil {
		log.Println("Failed to compress using zlib")
		return nil
	}

	// pushing actual body

	byteBuffer.Put(compressionByte.Bytes())

	return byteBuffer.Array()
}