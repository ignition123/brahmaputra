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


type HlsStruct struct{
	Hls_fragment int
	Hls_path string
	Hls_window int
}

type OnPublishStruct struct{
	AuthUrl string
	HookCall string
	Exec string
}

type OnPlayStruct struct{
	AuthUrl string
	HookCall string
	Exec string
}

type OnEndStruct struct{
	HookCall string
	Exec string
}

type RTMPChannelStruct struct{

	ChannelName string
	Hls HlsStruct
	OnEnd OnEndStruct
	OnPublish OnPublishStruct
	OnPlay OnPlayStruct

}