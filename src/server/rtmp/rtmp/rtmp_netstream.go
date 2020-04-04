package rtmp

import (
	"server/rtmp/avformat"
	"server/rtmp/hls"
	"server/rtmp/mpegts"
	"server/rtmp/util"
	"bytes"
	"fmt"
	"io"
	"os"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"
	"ChannelList"
	"net/http"
	"log"
)

type NetStream interface{

	AttachVideo(video chan *AVPacket)
	AttachAudio(audio chan *AVPacket)  
	Publish(streamName string, streamType string) error
	Play(streamName string, args ...interface{}) error 
	Pause()                                           
	Resume()                                           
	TogglePause()                                      
	Seek(offset uint64)                            
	Send(handlerName string, args ...interface{})     
	Close()                                         
	ReceiveAudio(flag bool)                           
	ReceiveVideo(flag bool)                           
	NetConnection() NetConnection                     
	BufferTime() time.Duration                        
	BytesLoaded() uint64                              
	BufferLength() uint64  

}

type RtmpNetStream struct{

	conn           *RtmpNetConnection
	metaData       *AVPacket       
	videoTag       *AVPacket      
	audioTag       *AVPacket       
	videoKeyFrame  *AVPacket        
	videochan      chan *AVPacket    
	audiochan      chan *AVPacket    
	streamPath     string      
	appName        string
	key            string
	bufferTime     time.Duration    
	bufferLength   uint64          
	bufferLoad     uint64           
	lock           *sync.Mutex    
	serverHandler  ServerHandler   
	mode           int              
	vkfsended      bool           
	akfsended      bool           
	vstreamToFile  bool             
	astreamToFile  bool              
	broadcast      *Broadcast       
	total_duration uint32          
	vsend_time     uint32           
	asend_time     uint32           
	closed         bool            
	rtmpFile       *RtmpFile    

}

func newNetStream(conn *RtmpNetConnection, sh ServerHandler) (s *RtmpNetStream){
	s = new(RtmpNetStream)
	s.conn = conn
	s.lock = new(sync.Mutex)
	s.serverHandler = sh
	s.vkfsended = false
	s.akfsended = false
	s.vstreamToFile = false
	s.astreamToFile = false
	return

}

func (s *RtmpNetStream) AttachVideo(video chan *AVPacket){

	s.videochan = video

}

func (s *RtmpNetStream) AttachAudio(audio chan *AVPacket){

	s.audiochan = audio

}

func (s *RtmpNetStream) SendVideo(video *AVPacket) error{

	if s.vkfsended{

		video.Timestamp -= s.vsend_time - uint32(s.bufferTime)

		s.vsend_time += video.Timestamp

		return sendMessage(s.conn, SEND_VIDEO_MESSAGE, video)

	}

	if !video.isKeyFrame(){

		return nil

	}

	vTag := s.broadcast.publisher.videoTag

	if vTag == nil{

		fmt.Println("Video Tag nil")

		return nil

	}

	vTag.Timestamp = 0

	version := vTag.Payload[4+1]                                           
	avcPfofile := vTag.Payload[4+2]                                       
	profileCompatibility := vTag.Payload[4+3]                              
	avcLevel := vTag.Payload[4+4]                                         
	reserved := vTag.Payload[4+5] >> 2                                     
	lengthSizeMinusOne := vTag.Payload[4+5] & 0x03                        
	reserved2 := vTag.Payload[4+6] >> 5                                   
	numOfSPS := vTag.Payload[4+6] & 31                                     
	spsLength := util.BigEndian.Uint16(vTag.Payload[4+7:])                 
	sps := vTag.Payload[4+9 : 4+9+int(spsLength)]                          
	numOfPPS := vTag.Payload[4+9+int(spsLength)]                          
	ppsLength := util.BigEndian.Uint16(vTag.Payload[4+9+int(spsLength)+1:]) 
	pps := vTag.Payload[4+9+int(spsLength)+1+2:]             

	fmt.Sprintf("ConfigurationVersion:%v\nAVCProfileIndication:%v\nprofile_compatibility/%v\nAVCLevelIndication/%v\nreserved/%v\nlengthSizeMinusOne/%v\nreserved2/%v\nNumOfSequenceParameterSets/%v\nSequenceParameterSetLength/%v\nSPS/%02x\nNumOfPictureParameterSets/%v\nPictureParameterSetLength/%v\nPPS/%02x\n",
		version, avcPfofile, profileCompatibility, avcLevel, reserved, lengthSizeMinusOne, reserved2, numOfSPS, spsLength, sps, numOfPPS, ppsLength, pps)

	err := sendMessage(s.conn, SEND_FULL_VDIEO_MESSAGE, vTag)

	if err != nil{

		return err

	}

	s.vkfsended = true
	s.vsend_time = video.Timestamp
	video.Timestamp = 0

	return sendMessage(s.conn, SEND_FULL_VDIEO_MESSAGE, video)
}

func (s *RtmpNetStream) SendAudio(audio *AVPacket) error{

	if !s.vkfsended{

		return nil

	}

	if s.akfsended{

		audio.Timestamp -= s.asend_time - uint32(s.bufferTime)
		s.asend_time += audio.Timestamp                      
		return sendMessage(s.conn, SEND_AUDIO_MESSAGE, audio)
	
	}

	aTag := s.broadcast.publisher.audioTag

	aTag.Timestamp = 0

	err := sendMessage(s.conn, SEND_FULL_AUDIO_MESSAGE, aTag)

	if err != nil{

		return err

	}

	s.akfsended = true                                       
	s.asend_time = audio.Timestamp                           
	audio.Timestamp = 0                                      
	return sendMessage(s.conn, SEND_FULL_AUDIO_MESSAGE, audio)

}

func (s *RtmpNetStream) WriteVideo(w io.Writer, video *AVPacket, fileType int) (err error){

	if ChannelList.RTMPStorage[s.conn.appName].Hls.Hls_path != ""{

		switch fileType{

			case RTMP_FILE_TYPE_ES_H264:
				{
					if s.rtmpFile.vtwrite{

						if _, err = w.Write(video.Payload); err != nil{

							return

						}

						return nil
					}

					if _, err = w.Write(s.videoTag.Payload); err != nil{

						return

					}

					s.rtmpFile.vtwrite = true
				}
			case RTMP_FILE_TYPE_TS:
				{
					if s.rtmpFile.vtwrite{

						var packet mpegts.MpegTsPESPacket

						if packet, err = rtmpVideoPacketToPES(video, s.rtmpFile.avc); err != nil{

							return

						}


						frame := new(mpegts.MpegtsPESFrame)

						frame.Pid = 0x101

						frame.IsKeyFrame = video.isKeyFrame()

						frame.ContinuityCounter = byte(s.rtmpFile.video_cc % 16)

						frame.ProgramClockReferenceBase = uint64(video.Timestamp) * 90

						if err = mpegts.WritePESPacket(w, frame, packet); err != nil{

							return

						}

						s.rtmpFile.video_cc = uint16(frame.ContinuityCounter)

						return nil
					}

					if s.rtmpFile.avc, err = decodeAVCDecoderConfigurationRecord(s.videoTag.Clone()); err != nil{

						return

					}

					if err = mpegts.WriteDefaultPATPacket(w); err != nil{

						return

					}

					if err = mpegts.WriteDefaultPMTPacket(w); err != nil{

						return

					}

					s.rtmpFile.vtwrite = true
				}
			case RTMP_FILE_TYPE_HLS_TS:
				{
					if s.rtmpFile.vtwrite{

						var packet mpegts.MpegTsPESPacket

						if packet, err = rtmpVideoPacketToPES(video, s.rtmpFile.avc); err != nil{

							return

						}

						if video.isKeyFrame(){

							if int64(video.Timestamp-s.rtmpFile.vwrite_time) >= s.rtmpFile.hls_fragment{

								tsFilename := strings.Split(s.streamPath, "/")[1] + "-" + strconv.FormatInt(time.Now().Unix(), 10) + ".ts"

								if err = writeHlsTsSegmentFile(s.rtmpFile.hls_path+"/"+tsFilename, s.rtmpFile.hls_segment_data.Bytes()); err != nil{

									return

								}

								inf := hls.PlaylistInf{

									Duration: float64((video.Timestamp - s.rtmpFile.vwrite_time) / 1000),

									Title:    tsFilename,

								}

								if s.rtmpFile.hls_segment_count >= uint32(ChannelList.RTMPStorage[s.conn.appName].Hls.Hls_window){

									if err = s.rtmpFile.hls_playlist.UpdateInf(s.rtmpFile.hls_m3u8_name, s.rtmpFile.hls_m3u8_name+".tmp", inf); err != nil{

										return

									}

								}else{

									if err = s.rtmpFile.hls_playlist.WriteInf(s.rtmpFile.hls_m3u8_name, inf); err != nil{

										return

									}

								}

								s.rtmpFile.hls_segment_count++
								s.rtmpFile.vwrite_time = video.Timestamp
								s.rtmpFile.hls_segment_data.Reset()

							}

						}

						frame := new(mpegts.MpegtsPESFrame)

						frame.Pid = 0x101

						frame.IsKeyFrame = video.isKeyFrame()

						frame.ContinuityCounter = byte(s.rtmpFile.video_cc % 16)

						frame.ProgramClockReferenceBase = uint64(video.Timestamp) * 90

						if err = mpegts.WritePESPacket(s.rtmpFile.hls_segment_data, frame, packet); err != nil{

							return

						}

						s.rtmpFile.video_cc = uint16(frame.ContinuityCounter)

						return nil
					}

					if s.rtmpFile.avc, err = decodeAVCDecoderConfigurationRecord(s.videoTag.Clone()); err != nil{

						return

					}

					if ChannelList.RTMPStorage[s.conn.appName].Hls.Hls_fragment > 0{

						s.rtmpFile.hls_fragment = int64(ChannelList.RTMPStorage[s.conn.appName].Hls.Hls_fragment) * 1000

					}else{

						s.rtmpFile.hls_fragment = 10000

					}

					s.rtmpFile.hls_playlist = hls.Playlist{

						Version:        3,

						Sequence:       0,

						Targetduration: int(s.rtmpFile.hls_fragment / 666),

					}

					s.rtmpFile.hls_path = ChannelList.RTMPStorage[s.conn.appName].Hls.Hls_path + "/" + strings.Split(s.streamPath, "/")[0]+"/"+strings.Split(s.streamPath, "/")[1]

					s.rtmpFile.hls_m3u8_name = s.rtmpFile.hls_path +"/myStream.m3u8"

					if !util.Exist(s.rtmpFile.hls_path){

						if err = os.Mkdir(s.rtmpFile.hls_path, os.ModePerm); err != nil{

							return

						}

					}

					if err = s.rtmpFile.hls_playlist.Init(s.rtmpFile.hls_m3u8_name); err != nil{

						fmt.Println(err)

						return

					}

					s.rtmpFile.hls_segment_data = &bytes.Buffer{}

					s.rtmpFile.hls_segment_count = 0

					s.rtmpFile.vtwrite = true

				}
			case RTMP_FILE_TYPE_FLV:
				{
					if s.rtmpFile.vtwrite{

						if err = writeFLVTag(w, video); err != nil{

							return

						}

						return nil
					}

					header := avformat.FLVHeader{

						SignatureF:     0x46,
						SignatureL:     0x4C,
						SignatureV:     0x56,
						Version:        0x01,
						TypeFlagsAudio: 1,
						TypeFlagsVideo: 1,
						DataOffse:      9,

					}

					if err = avformat.WriteFLVHeader(w, header); err != nil{

						return

					}

					if err = util.WriteUint32ToByte(w, 0x00000000, true); err != nil{

						return

					}

					if err = writeFLVTag(w, s.videoTag.Clone()); err != nil{

						return

					}

					s.rtmpFile.vtwrite = true
				}
			case RTMP_FILE_TYPE_MP4:
				{
				}
			default:
				{
					err = errors.New("unknow video file type.")
					return
				}
			}

			return nil
			
	}

	return nil
}

func (s *RtmpNetStream) WriteAudio(w io.Writer, audio *AVPacket, fileType int) (err error){

	switch fileType{

	case RTMP_FILE_TYPE_ES_AAC:
		{
			if s.rtmpFile.atwrite{

				if _, err = w.Write(audio.Payload); err != nil{

					return

				}

				return nil

			}

			if _, err = w.Write(s.audioTag.Payload); err != nil{

				return

			}

			s.rtmpFile.atwrite = true

		}
	case RTMP_FILE_TYPE_TS:
		{

			if !s.rtmpFile.vtwrite{

				return nil

			}

			if s.rtmpFile.atwrite{

				var packet mpegts.MpegTsPESPacket

				if packet, err = rtmpAudioPacketToPES(audio, s.rtmpFile.asc); err != nil{

					return

				}

				frame := new(mpegts.MpegtsPESFrame)

				frame.Pid = 0x102

				frame.IsKeyFrame = false

				frame.ContinuityCounter = byte(s.rtmpFile.audio_cc % 16)

				if err = mpegts.WritePESPacket(w, frame, packet); err != nil{

					return

				}

				s.rtmpFile.audio_cc = uint16(frame.ContinuityCounter)

				return nil
			}

			if s.rtmpFile.asc, err = decodeAudioSpecificConfig(s.audioTag.Clone()); err != nil{

				return

			}

			s.rtmpFile.atwrite = true

		}
	case RTMP_FILE_TYPE_HLS_TS:
		{
		
			if !s.rtmpFile.vtwrite{

				return nil

			}

			if s.rtmpFile.atwrite{

				var packet mpegts.MpegTsPESPacket

				if packet, err = rtmpAudioPacketToPES(audio, s.rtmpFile.asc); err != nil{

					return

				}

				frame := new(mpegts.MpegtsPESFrame)

				frame.Pid = 0x102

				frame.IsKeyFrame = false

				frame.ContinuityCounter = byte(s.rtmpFile.audio_cc % 16)

				if err = mpegts.WritePESPacket(s.rtmpFile.hls_segment_data, frame, packet); err != nil{

					return

				}

				s.rtmpFile.audio_cc = uint16(frame.ContinuityCounter)

				return nil
			}

			if s.rtmpFile.asc, err = decodeAudioSpecificConfig(s.audioTag.Clone()); err != nil{

				return

			}

			s.rtmpFile.atwrite = true
		}
	case RTMP_FILE_TYPE_FLV:
		{

			if !s.rtmpFile.vtwrite{

				return nil

			}

			if s.rtmpFile.atwrite{

				if err = writeFLVTag(w, audio); err != nil{

					return

				}

				return nil

			}

			if err = writeFLVTag(w, s.audioTag.Clone()); err != nil{

				return

			}

			s.rtmpFile.atwrite = true

		}
	case RTMP_FILE_TYPE_MP4:
		{
		}
	default:
		{

			err = errors.New("unknow audio file type.")

			return

		}

	}

	return nil
}

func (s *RtmpNetStream) Close(){

	s.lock.Lock()

	defer s.lock.Unlock()

	if s.closed{

		return

	}

	s.conn.Close()

	s.closed = true

	s.serverHandler.OnClosed(s)

}

func (s *RtmpNetStream) msgLoopProc(){

	// createStreamMessageHandle
	// 0
	// publishMessageHandle
	// 1
	// &{0xc0000e2000 <nil> <nil> <nil> <nil> <nil> <nil> sudeep/abhik/brahm/sudeepdhaksd/dsadsadsa 0 0 0 0xc0000e01a8 0x6a15c8 0 false false false false <nil> 0 0 0 false <nil>}
	// &{0xc0000e2000 <nil> <nil> <nil> <nil> <nil> <nil> sudeep/abhik/brahm/sudeepdhaksd/dsadsadsa 0 0 0 0xc0000e01a8 0x6a15c8 0 false false false false <nil> 0 0 0 false <nil>}
	// metadataMessageHandle
	// 2
	// audioMessageHandle
	// 3
	// videoMessageHandle
	// 4
	// videoMessageHandle
	// 5

	for{

		msg, err := recvMessage(s.conn)

		if err != nil{

			s.serverHandler.OnError(s, err)

			break

		}

		if msg.Header().ChunkMessgaeHeader.MessageLength <= 0{

			continue

		}

		msgID := msg.Header().ChunkMessgaeHeader.MessageTypeID

		if msgID == RTMP_MSG_AUDIO || msgID == RTMP_MSG_VIDEO{

		}else{

			fmt.Println(msg.String())

		}

		switch v := msg.(type){

		case *AudioMessage:
			{
				// executed so on along with video

				audioMessageHandle(s, v)

			}
		case *VideoMessage:
			{

				// executed so on along with audio

				videoMessageHandle(s, v)

			}
		case *MetadataMessage:
			{

				// executed third

				metadataMessageHandle(s, v)

			}
		case *CreateStreamMessage:
			{

				// executed first

				if err := createStreamMessageHandle(s, v); err != nil{

					s.serverHandler.OnError(s, err)

					return

				}

			}
		case *PublishMessage:
			{

				// executed second

				if err := publishMessageHandle(s, v); err != nil{

					s.serverHandler.OnError(s, err)

					return

				}

			}
		case *PlayMessage:
			{
				// executed when the rtmp is pulled

				if err := playMessageHandle(s, v); err != nil{

					s.serverHandler.OnError(s, err)

					return

				}

			}
		case *ReleaseStreamMessage:
			{
				// TODO:
			}
		case *CloseStreamMessage:
			{

				s.Close()

			}
		case *FCPublishMessage:
			{
				// // TODO
				// if err := fcPublishMessageHandle(s, v); err != nil {
				// 	return
				// }
			}
		case *FCUnpublishMessage:
			{
				// // TODO
				// if err := fcUnPublishMessageHandle(s, v); err != nil {
				// 	return
				// }
			}
		default:
			{
				fmt.Println("Other Message :", v)
			}

		}

	}

}

func audioMessageHandle(s *RtmpNetStream, audio *AudioMessage){

	pkt := new(AVPacket)

	if audio.RtmpHeader.ChunkMessgaeHeader.Timestamp == 0xffffff{

		s.total_duration += audio.RtmpHeader.ChunkExtendedTimestamp.ExtendTimestamp

		pkt.Timestamp = s.total_duration

	}else{

		s.total_duration += audio.RtmpHeader.ChunkMessgaeHeader.Timestamp 

		pkt.Timestamp = s.total_duration                                

	}

	pkt.Type = audio.RtmpHeader.ChunkMessgaeHeader.MessageTypeID

	pkt.Payload = audio.RtmpBody.Payload

	tmp := pkt.Payload[0]           
	pkt.SoundFormat = tmp >> 4      
	pkt.SoundRate = (tmp & 0x0c) >> 2
	pkt.SoundSize = (tmp & 0x02) >> 1
	pkt.SoundType = tmp & 0x01    

	if s.audioTag == nil{

		s.audioTag = pkt

	}else{

		s.audiochan <- pkt

	}

}

func videoMessageHandle(s *RtmpNetStream, video *VideoMessage){

	pkt := new(AVPacket)

	if video.RtmpHeader.ChunkMessgaeHeader.Timestamp == 0xffffff{

		s.total_duration += video.RtmpHeader.ChunkExtendedTimestamp.ExtendTimestamp

		pkt.Timestamp = s.total_duration

	}else{

		s.total_duration += video.RtmpHeader.ChunkMessgaeHeader.Timestamp

		pkt.Timestamp = s.total_duration

	}

	pkt.Type = video.RtmpHeader.ChunkMessgaeHeader.MessageTypeID

	pkt.Payload = video.RtmpBody.Payload

	tmp := pkt.Payload[0]    

	pkt.VideoFrameType = tmp >> 4

	pkt.VideoCodecID = tmp & 0x0f 

	if s.videoTag == nil{

		s.videoTag = pkt

	}else{

		if pkt.VideoFrameType == 1{

			s.videoKeyFrame = pkt

		}

		s.videochan <- pkt

	}

}

func metadataMessageHandle(s *RtmpNetStream, mete *MetadataMessage){

	pkt := new(AVPacket)

	pkt.Timestamp = mete.RtmpHeader.ChunkMessgaeHeader.Timestamp

	if mete.RtmpHeader.ChunkMessgaeHeader.Timestamp == 0xffffff{

		pkt.Timestamp = mete.RtmpHeader.ChunkExtendedTimestamp.ExtendTimestamp

	}

	pkt.Type = mete.RtmpHeader.ChunkMessgaeHeader.MessageTypeID

	pkt.Payload = mete.RtmpBody.Payload

	if s.metaData == nil{

		s.metaData = pkt

	}

}

func createStreamMessageHandle(s *RtmpNetStream, csmsg *CreateStreamMessage) error{

	s.conn.streamID = s.conn.nextStreamID(csmsg.RtmpHeader.ChunkBasicHeader.ChunkStreamID)

	return sendMessage(s.conn, SEND_CREATE_STREAM_RESPONSE_MESSAGE, csmsg.TransactionId)

}

func authRequest(s *RtmpNetStream, app string, key string, url string) bool{
	
	client := http.Client{
		Timeout: 5 * time.Second,
	}

	request, err := http.NewRequest("GET", url, nil)

	if err != nil {

		log.Print(err)

        s.conn.conn.Close()

        return false

	}

	q := request.URL.Query()

    q.Add("app", app)

    q.Add("key", key)

    request.URL.RawQuery = q.Encode()

	resp, err := client.Do(request)

	if err != nil {

		log.Print(err)

        s.conn.conn.Close()

        return false

	}

	defer resp.Body.Close()

	if resp.StatusCode != 200{

		s.conn.conn.Close()

		return false

	}

    return true

}

func hookRequest(s *RtmpNetStream, app string, key string, url string) bool{
	
	client := http.Client{
		Timeout: 5 * time.Second,
	}

	request, err := http.NewRequest("GET", url, nil)

	if err != nil {

		log.Print(err)

        s.conn.conn.Close()

        return false

	}

	q := request.URL.Query()

    q.Add("app", app)

    q.Add("key", key)

    request.URL.RawQuery = q.Encode()

	resp, err := client.Do(request)

	if err != nil {

		log.Print(err)

        s.conn.conn.Close()

        return false

	}

	defer resp.Body.Close()

	if resp.StatusCode != 200{

		s.conn.conn.Close()

		return false

	}

    return true

}

func publishMessageHandle(s *RtmpNetStream, pbmsg *PublishMessage) error{

	if strings.HasSuffix(s.conn.appName, "/"){

		s.streamPath = s.conn.appName + strings.Split(pbmsg.PublishingName, "?")[0]

	}else{

		s.streamPath = s.conn.appName + "/" + strings.Split(pbmsg.PublishingName, "?")[0]

	}

	s.appName = s.conn.appName

	s.key = pbmsg.PublishingName

	// here authentication implementation will be done

	if ChannelList.RTMPStorage[s.conn.appName].OnPublish.AuthUrl != ""{

		if !authRequest(s, s.conn.appName, pbmsg.PublishingName, ChannelList.RTMPStorage[s.conn.appName].OnPublish.AuthUrl){

			return nil

		}

	}

	// hook event implementation will be done

	if ChannelList.RTMPStorage[s.conn.appName].OnPublish.HookCall != ""{

		if !hookRequest(s, s.conn.appName, pbmsg.PublishingName, ChannelList.RTMPStorage[s.conn.appName].OnPublish.HookCall){

			return nil

		}

	}

	err := s.serverHandler.OnPublishing(s)

	if err != nil{

		prmdErr := newPublishResponseMessageData(s.conn.streamID, "error", err.Error())

		err = sendMessage(s.conn, SEND_PUBLISH_RESPONSE_MESSAGE, prmdErr)

		if err != nil{

			return err

		}

		return nil
	}

	err = sendMessage(s.conn, SEND_STREAM_BEGIN_MESSAGE, nil)

	if err != nil{

		return err

	}

	prmdStart := newPublishResponseMessageData(s.conn.streamID, NetStream_Publish_Start, Level_Status)

	err = sendMessage(s.conn, SEND_PUBLISH_START_MESSAGE, prmdStart)

	if err != nil{

		return err

	}

	if s.mode == 0{

		s.mode = 1

	}else{

		s.mode = s.mode | 1

	}

	return nil

}

func playMessageHandle(s *RtmpNetStream, plmsg *PlayMessage) error{

	if strings.HasSuffix(s.conn.appName, "/"){

		s.streamPath = s.conn.appName + strings.Split(plmsg.StreamName, "?")[0]

	}else{

		s.streamPath = s.conn.appName + "/" + strings.Split(plmsg.StreamName, "?")[0]

	}

	// here authentication implementation will be done

	if ChannelList.RTMPStorage[s.conn.appName].OnPlay.AuthUrl != ""{

		if !authRequest(s, s.conn.appName, plmsg.StreamName, ChannelList.RTMPStorage[s.conn.appName].OnPlay.AuthUrl){

			return nil

		}

	}

	// hook event implementation will be done

	if ChannelList.RTMPStorage[s.conn.appName].OnPlay.HookCall != ""{

		if !hookRequest(s, s.conn.appName, plmsg.StreamName, ChannelList.RTMPStorage[s.conn.appName].OnPlay.HookCall){

			return nil

		}

	}

	fmt.Println("stream path:", s.streamPath)

	s.conn.writeChunkSize = 512 

	err := s.serverHandler.OnPlaying(s)

	if err != nil{

		prmdErr := newPlayResponseMessageData(s.conn.streamID, "error", err.Error())

		err = sendMessage(s.conn, SEND_PLAY_RESPONSE_MESSAGE, prmdErr) 

		if err != nil{

			return err

		}

	}

	err = sendMessage(s.conn, SEND_CHUNK_SIZE_MESSAGE, uint32(s.conn.writeChunkSize))

	if err != nil{

		return err

	}

	err = sendMessage(s.conn, SEND_STREAM_IS_RECORDED_MESSAGE, nil)

	if err != nil{

		return err

	}

	sendMessage(s.conn, SEND_STREAM_BEGIN_MESSAGE, nil)

	if err != nil{

		return err

	}

	prmdReset := newPlayResponseMessageData(s.conn.streamID, NetStream_Play_Reset, Level_Status)

	err = sendMessage(s.conn, SEND_PLAY_RESPONSE_MESSAGE, prmdReset)

	if err != nil{

		return err

	}

	prmdStart := newPlayResponseMessageData(s.conn.streamID, NetStream_Play_Start, Level_Status)

	err = sendMessage(s.conn, SEND_PLAY_RESPONSE_MESSAGE, prmdStart)

	if err != nil{

		return err

	}

	if s.mode == 0{

		s.mode = 2

	}else{

		s.mode = s.mode | 2

	}

	return nil
}

func fcPublishMessageHandle(s *RtmpNetStream, fcpmsg *FCPublishMessage) (err error){

	return

}


func fcUnPublishMessageHandle(s *RtmpNetStream, fcunpmsg *FCUnpublishMessage) (err error){

	// resData := newAMFObjects()
	// resData["code"] = NetStream_Unpublish_Success
	// resData["level"] = Level_Status
	// resData["streamid"] = conn.streamID
	// err = sendMessage(conn, SEND_UNPUBLISH_RESPONSE_MESSAGE, resData)
	// if err != nil {
	// 	return
	// }

	return
}

func decodeMetadataMessage(metadata *AVPacket){

	if len(metadata.Payload) <= 0{

		return

	}

	amf := newAMFDecoder(metadata.Payload)

	objs, _ := amf.readObjects()

	for _, v := range objs{

		switch tt := v.(type){

		case string:
			{
				fmt.Println("string :", tt)
			}
		case []byte:
			{
				fmt.Println("[]byte :", tt)
			}
		case byte:
			{
				fmt.Println("byte :", tt)
			}
		case int:
			{
				fmt.Println("int :", tt)
			}
		case float64:
			{
				fmt.Println("float64 :", tt)
			}
		case AMFObjects:
			{
				for i, v1 := range tt {
					fmt.Println(i, " : ", v1)
				}
			}
		default:
			{
				fmt.Println("default", tt)
			}

		}

	}
	
}
