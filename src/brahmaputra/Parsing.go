package brahmaputra

/*
	Parsing incomming messages from the server
*/

// importing the modules in golang 

import(
	"ByteBuffer"
	"encoding/json"
	"encoding/binary"
	"log"
)

// parse incomming message

func (e *CreateProperties) parseMsg(packetSize int64, message []byte, msgType string, callbackChan chan string){

	defer handlepanic()

	// checking for message type, if publisher

	if msgType == "pub"{

		producer_id := string(message)

		callbackChan <- string(producer_id)

		return
	}

	// checking for message type, if subscriber

	if msgType == "sub"{	

		// creating a byte buffer in big endian

		byteBuffer := ByteBuffer.Buffer{
			Endian:"big",
		}

		// wrapping the current message to byte buffer

		byteBuffer.Wrap(message)

		// parsing the message type

		messageTypeLen := int(binary.BigEndian.Uint16(byteBuffer.GetShort()))
		messageType := byteBuffer.Get(messageTypeLen)

		if string(messageType) == "FIN"{

			if e.AutoAcknowledge{

				e.Commit()
			}

			callbackChan <- "SUCCESS"

			return

		}

		// parsing the channelName

		channelNameLen := int(binary.BigEndian.Uint16(byteBuffer.GetShort()))
		byteBuffer.Get(channelNameLen)

		// parsing the producer length

		producer_idLen := int(binary.BigEndian.Uint16(byteBuffer.GetShort()))
		byteBuffer.Get(producer_idLen)

		// parsing the agent name length

		agentNameLen := int(binary.BigEndian.Uint16(byteBuffer.GetShort()))
		byteBuffer.Get(agentNameLen)

		// getting the id

		byteBuffer.GetLong() //id

		// getting the compression type 

		compression := byteBuffer.GetByte()

		// getting the actual body packet size

		bodyPacketSize := packetSize - int64(2 + messageTypeLen + 2 + channelNameLen + 2 + producer_idLen + 2 + agentNameLen + 8 + 1)

		bodyPacket := byteBuffer.Get(int(bodyPacketSize))

		// parsing the body packet

		if compression[0] == 2{

			// zlib compression

			bodyPacket = zlibCompressionReadMethod(bodyPacket)

		}else if compression[0] == 3{

			// gzip compression

			bodyPacket = gzipCompressionReadMethod(bodyPacket)

		}else if compression[0] == 4{

			// snappy compression

			bodyPacket = snappyCompressionReadMethod(bodyPacket)

		}else if compression[0] == 5{

			// lz4 compression

			bodyPacket = lz4CompressionReadMethod(bodyPacket)

		}

		// if content matcher map is != 0 then parsing the message with the content match

		if len(e.contentMatcherMap) != 0{

			// messageData hashmap to map the body

			messageData := make(map[string]interface{})

			errJson := json.Unmarshal(bodyPacket, &messageData)

			if errJson != nil{
				
				go log.Println(errJson)
					
				callbackChan <- "REJECT"

				return

			}

			// matching with the content matcher

			e.contentMatch(messageData)

		}else{

			SubscriberChannel <- bodyPacket

		}

		callbackChan <- "SUCCESS"	
	}
}

// match the body packet with the content Match

func (e *CreateProperties) contentMatch(messageData map[string]interface{}){

	matchFound := true

	if _, found := e.contentMatcherMap["$and"]; found {

	    matchFound = andMatch(messageData, e.contentMatcherMap)

	}else if _, found := e.contentMatcherMap["$or"]; found {

		matchFound = orMatch(messageData, e.contentMatcherMap)

	}else if _, found := e.contentMatcherMap["$eq"]; found {

		if e.contentMatcherMap["$eq"] == "all"{

			matchFound = true

		}else{

			matchFound = false

		}

	}else{

		matchFound = false

	}

	if matchFound{

		SubscriberChannel <- messageData

	}

}