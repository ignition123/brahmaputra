package rtmp

import (
	"fmt"
	"sync"
	"time"
)

var (
	broadcasts = make(map[string]*Broadcast)
)


type Broadcast struct{

	lock       *sync.Mutex               // lock
	publisher  *RtmpNetStream            // 
	subscriber map[string]*RtmpNetStream // 
	streamPath string                    //
	control    chan interface{}          //

}

type AVChannel struct{

	id    string
	audio chan *AVPacket
	video chan *AVPacket

}

func find_broadcast(path string) (*Broadcast, bool){

	v, ok := broadcasts[path]

	return v, ok

}

func start_broadcast(publisher *RtmpNetStream, vl, al int){

	av := &AVChannel{

		id:    publisher.conn.remoteAddr,
		audio: make(chan *AVPacket, al), 
		video: make(chan *AVPacket, vl),

	} 

	publisher.AttachAudio(av.audio) 

	publisher.AttachVideo(av.video) 

	b := &Broadcast{

		streamPath: publisher.streamPath,               
		lock:       new(sync.Mutex),                   
		publisher:  publisher,                          
		subscriber: make(map[string]*RtmpNetStream, 0),
		control:    make(chan interface{}, 10),

	}       

	broadcasts[publisher.streamPath] = b 

	b.start()
}

func (b *Broadcast) addSubscriber(s *RtmpNetStream){

	s.broadcast = b

	b.control <- s

}

func (b *Broadcast) removeSubscriber(s *RtmpNetStream){

	s.closed = true

	b.control <- s

}

func (b *Broadcast) stop(){

	delete(broadcasts, b.streamPath)

	b.control <- "stop"

}

func (b *Broadcast) start(){

	go func(b *Broadcast){

		defer func(){

			if e := recover(); e != nil{

				fmt.Println(e)

			}

			fmt.Println("Broadcast :" + b.streamPath + " stopped")

		}()

		d := time.Now().Sub(begintime)

		fmt.Printf("------------Intreval Time :%v ------------\n", d)

		b.publisher.astreamToFile = true

		b.publisher.vstreamToFile = true

		b.publisher.rtmpFile = newRtmpFile()

		for{

			select{

			case amsg := <-b.publisher.audiochan:
				{
					for _, s := range b.subscriber{ 

						err := s.SendAudio(amsg.Clone())

						if err != nil{

							s.serverHandler.OnError(s, err)

						}

					}

					// write file
					if b.publisher.astreamToFile{

						err := b.publisher.WriteAudio(nil, amsg.Clone(), RTMP_FILE_TYPE_HLS_TS)

						if err != nil{

							// handler error

							fmt.Println("wirte audio file error :", err)

						}

					}

				}
			case vmsg := <-b.publisher.videochan:
				{
					for _, s := range b.subscriber{ 

						err := s.SendVideo(vmsg.Clone())

						if err != nil{

							s.serverHandler.OnError(s, err)

						}

					}

					// write file
					if b.publisher.vstreamToFile{

						err := b.publisher.WriteVideo(nil, vmsg.Clone(), RTMP_FILE_TYPE_HLS_TS)

						if err != nil{

							// handler error

							fmt.Println("wirte video file error :", err)

						}

					}		
					
				}
			case obj := <-b.control:
				{
					if c, ok := obj.(*RtmpNetStream); ok{

						if c.closed{

							delete(b.subscriber, c.conn.remoteAddr)

							fmt.Println("Subscriber Closed, Broadcast :", b.streamPath, "\nSubscribe :", len(b.subscriber))

						}else{

							b.subscriber[c.conn.remoteAddr] = c                                                

							fmt.Println("Subscriber Open, Broadcast :", b.streamPath, "\nSubscribe :", len(b.subscriber)) 

						}

					} else if v, ok := obj.(string); ok && "stop" == v{

						for k, ss := range b.subscriber{ 

							delete(b.subscriber, k) 

							ss.Close()      

						}

					}

				}
			case <-time.After(time.Second * 100):
				{
					fmt.Println("Broadcast " + b.streamPath + " Video | Audio Buffer Empty,Timeout 100s")

					b.stop()

					b.publisher.Close()

					return
					
				}
			}
		}
	}(b)
}
