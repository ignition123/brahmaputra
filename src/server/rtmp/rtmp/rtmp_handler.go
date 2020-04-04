package rtmp

import (
	"errors"
	"fmt"
	"ChannelList"
)

type ServerHandler interface{

	OnPublishing(s *RtmpNetStream) error
	OnPlaying(s *RtmpNetStream) error
	OnClosed(s *RtmpNetStream)
	OnError(s *RtmpNetStream, err error)

}

type DefaultServerHandler struct{

}

func (p *DefaultServerHandler) OnPublishing(s *RtmpNetStream) error{

	if _, ok := find_broadcast(s.streamPath); ok{

		return errors.New("NetStream.Publish.BadName")

	}

	start_broadcast(s, 5, 5)

	return nil
}

func (p *DefaultServerHandler) OnPlaying(s *RtmpNetStream) error{

	if d, ok := find_broadcast(s.streamPath); ok{

		d.addSubscriber(s)

		return nil

	}

	return errors.New("NetStream.Play.StreamNotFound")
}

func (dsh *DefaultServerHandler) OnClosed(s *RtmpNetStream){

	mode := "UNKNOWN"

	if s.mode == 2{

		mode = "CONSUMER"

	}else if s.mode == 3{

		mode = "PROXY"

	}else if s.mode == 2|1{

		mode = "PRODUCER|CONSUMER"

	}

	if ChannelList.RTMPStorage[s.conn.appName].OnEnd.HookCall != ""{

		hookRequest(s, s.appName, s.key, ChannelList.RTMPStorage[s.conn.appName].OnEnd.HookCall)

	}

	fmt.Printf("NetStream OnClosed, remoteAddr : %v\npath : %v\nmode : %v\n", s.conn.remoteAddr, s.streamPath, mode)

	if d, ok := find_broadcast(s.streamPath); ok{

		if s.mode == 1{

			d.stop()

		}else if s.mode == 2{

			d.removeSubscriber(s)

		}else if s.mode == 2|1{

			d.removeSubscriber(s)

			d.stop()

		}

	}

}

func (dsh *DefaultServerHandler) OnError(s *RtmpNetStream, err error){

	fmt.Printf("NetStream OnError, remoteAddr : %v\npath : %v\nerror : %v\n", s.conn.remoteAddr, s.streamPath, err)

	s.Close()
	
}
