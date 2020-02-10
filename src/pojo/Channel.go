package pojo

import(
	"net"
	"os"
)

type ChannelStruct struct{
	FD *os.File
	TableFD *os.File
	Path string
	Offset int64
	Worker int16
	BucketData [] chan map[string]interface{}
}

type SocketDetails struct{
	Conn net.Conn
	ContentMatcher map[string]interface{}
}
