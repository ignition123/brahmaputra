package pojo

import(
	"net"
	"os"
	"sync"
)

type ChannelStruct struct{
	FD []*os.File 
	Path string
	Offset int64
	Worker int
	BucketData [] chan *PacketStruct
	WriteCallback chan bool
	ChannelLock sync.RWMutex
	SubscriberChannelLock sync.RWMutex
	SubscriberFileChannelLock sync.RWMutex
	ChannelStorageType string
	SubscriberChannel []chan *PacketStruct
	PartitionCount int
	Group map[string][]*PacketStruct
	SubscriberList map[string]bool
}

type SocketDetails struct{
	Conn net.TCPConn
	ContentMatcher map[string]interface{}
}
