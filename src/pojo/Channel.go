package pojo

import(
	"net"
	"os"
	"sync"
)

type ChannelStruct struct{
	FD *os.File
	Path string
	Offset int64
	Worker int
	BucketData [] chan *PacketStruct
	WriteCallback chan bool
	ChannelLock sync.RWMutex
	ChannelStorageType string
}

type SocketDetails struct{
	Conn net.TCPConn
	ContentMatcher map[string]interface{}
}
