package rtmp

import (
	"bufio"
	"net"
	"sync"
	"time"
)

type NetConnection interface{

	Connect(command string, args ...interface{}) error
	Call(command string, args ...interface{}) error
	Close()
	Connected() bool
	URL() string

}

type RtmpNetConnection struct{
	appName			   string
	remoteAddr         string
	url                string
	server             *Server
	readChunkSize      int
	writeChunkSize     int
	createTime         string
	bandwidth          uint32
	readSeqNum         uint32                    
	writeSeqNum        uint32                     
	totalWrite         uint32                     
	totalRead          uint32                     
	objectEncoding     float64                  
	conn               net.Conn                   
	br                 *bufio.Reader              
	bw                 *bufio.Writer             
	brw                *bufio.ReadWriter         
	lock               *sync.Mutex                
	incompleteRtmpBody map[uint32][]byte         
	rtmpHeader         map[uint32]*RtmpHeader    
	connected          bool                      
	nextStreamID       func(chunkid uint32) uint32
	streamID           uint32                    

}

var gstreamid = uint32(64)

func gen_next_stream_id(chunkid uint32) uint32{

	gstreamid += 1

	return gstreamid

}

func newRtmpNetConnect(conn net.Conn, s *Server) (c *RtmpNetConnection){

	c = new(RtmpNetConnection)
	c.br = bufio.NewReader(conn)
	c.bw = bufio.NewWriter(conn)
	c.brw = bufio.NewReadWriter(c.br, c.bw)
	c.conn = conn
	c.lock = new(sync.Mutex)
	c.server = s
	c.bandwidth = RTMP_MAX_CHUNK_SIZE * 8
	c.createTime = time.Now().String()
	c.remoteAddr = conn.RemoteAddr().String()
	c.nextStreamID = gen_next_stream_id
	c.readChunkSize = RTMP_DEFAULT_CHUNK_SIZE
	c.writeChunkSize = RTMP_DEFAULT_CHUNK_SIZE
	c.rtmpHeader = make(map[uint32]*RtmpHeader)
	c.incompleteRtmpBody = make(map[uint32][]byte)
	c.objectEncoding = 0

	return

}

func (c *RtmpNetConnection) Connect(command string, args ...interface{}) error{

	return nil

}

func (c *RtmpNetConnection) Call(command string, args ...interface{}) error{

	return nil

}

func (c *RtmpNetConnection) Connected() bool{

	return c.connected

}

func (c *RtmpNetConnection) Close(){

	if c.conn == nil{

		return

	}

	c.lock.Lock()
	defer c.lock.Unlock()
	c.conn.Close()
	c.connected = false

}

func (c *RtmpNetConnection) URL() string{

	return c.url
	
}
