package pojo

import(
	"net"
)

type ChannelStruct struct{
	Path string
	WriteInterval int32
	BucketData chan map[string]interface{}
}

type SocketDetails struct{
	Conn net.Conn
	ContentMatcher map[string]interface{}
}
