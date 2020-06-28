package tcp

/*
	This file contains all publishing from server to client methods

	It contains both inmemory as well as persistent streaming methods

	original @author Sudeep Dasgupta
*/

// importing modules

import(
	"sync"
	"ChannelList"
	"time"
	"ByteBuffer"
	"pojo"
)

// declaring a struct with a mutex

type ChannelMethods struct{
	sync.Mutex
}

// initializing a structure globally

var ChannelMethod = &ChannelMethods{}

// creating channels for inmemory subscription

func (e *ChannelMethods) GetChannelData(){

	defer ChannelList.Recover()

	for channelName := range ChannelList.TCPStorage {

	    go e.runChannel(channelName)
	}
}

// initializing channels

func (e *ChannelMethods) runChannel(channelName string){

	defer ChannelList.Recover()

	// iterating over the channel bucket and initializing

	for index := range ChannelList.TCPStorage[channelName].BucketData{

		time.Sleep(100)

		// using go routines for starting infinite loops

		go func(index int, BucketData chan *pojo.PacketStruct, channelName string){

			defer ChannelList.Recover()
			defer close(BucketData)

			msgChan := make(chan bool, 1)
			defer close(msgChan)

			// infinitely listening to channels

			for{

				select {

					case message, ok := <-BucketData:	

						if ok{

							// if messages arives then

							subchannelName := message.ChannelName

							// checking for not heart_beat channels

							if(channelName == subchannelName && channelName != "heart_beat"){

								// publishing to all subsriber

								go e.sendMessageToClient(*message, msgChan)

								// waiting for channel callback

								<-msgChan

							}
						}		
					break
				}		
			}

		}(index, ChannelList.TCPStorage[channelName].BucketData[index], channelName)
	}
}

// Producer Acknowledgement method

func (e *ChannelMethods) SendAck(messageMap pojo.PacketStruct, ackChan chan bool){

	defer ChannelList.Recover()

	// creating byte buffer to send acknowledgement to producer

	byteBuffer := ByteBuffer.Buffer{
		Endian:"big",
	}

	byteBuffer.PutLong(len(messageMap.Producer_id))

	byteBuffer.PutByte(byte(2)) // status code

	byteBuffer.Put([]byte(messageMap.Producer_id))

	// writing to tcp socket

	_, err := messageMap.Conn.Write(byteBuffer.Array())

	if err != nil{

		go ChannelList.WriteLog(err.Error())

		ackChan <- false

		return
	}

	ackChan <- true

}

// to be member for the future development contact @sudeep@pounze.com