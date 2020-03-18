package pojo

import(
	"net"
	"os"
	"sync"
)

type ChannelStruct struct{
	FD *os.File
	TableFD *os.File
	Path string
	Offset int64
	Worker int
	BucketData [] chan map[string]interface{}
	WriteCallback chan bool
	WriteOffsetCallback chan bool
	WriteMongoCallback chan bool
	sync.Mutex
}

type SocketDetails struct{
	Conn net.TCPConn
	ContentMatcher map[string]interface{}
}
