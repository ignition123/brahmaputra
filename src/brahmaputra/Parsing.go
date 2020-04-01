package brahmaputra

import(
	"ByteBuffer"
	"encoding/json"
	"encoding/binary"
	"log"
)

func (e *CreateProperties) parseMsg(packetSize int64, message []byte, msgType string, callbackChan chan string){

	defer handlepanic()

	if msgType == "pub"{

		var producer_id = string(message)

		callbackChan <- string(producer_id)

	}

	if msgType == "sub"{	

		var byteBuffer = ByteBuffer.Buffer{
			Endian:"big",
		}

		byteBuffer.Wrap(message)

		var messageTypeByte = byteBuffer.GetShort()
		var messageTypeLen = int(binary.BigEndian.Uint16(messageTypeByte))
		byteBuffer.Get(messageTypeLen)

		var channelNameByte = byteBuffer.GetShort()
		var channelNameLen = int(binary.BigEndian.Uint16(channelNameByte))
		byteBuffer.Get(channelNameLen)

		var producer_idByte = byteBuffer.GetShort()
		var producer_idLen = int(binary.BigEndian.Uint16(producer_idByte))
		byteBuffer.Get(producer_idLen)

		var agentNameByte = byteBuffer.GetShort()
		var agentNameLen = int(binary.BigEndian.Uint16(agentNameByte))
		byteBuffer.Get(agentNameLen)

		byteBuffer.GetLong() //id

		var bodyPacketSize = packetSize - int64(2 + messageTypeLen + 2 + channelNameLen + 2 + producer_idLen + 2 + agentNameLen + 8)

		var bodyPacket = byteBuffer.Get(int(bodyPacketSize))

		if len(e.contentMatcherMap) != 0{

			var messageData = make(map[string]interface{})

			errJson := json.Unmarshal(bodyPacket, &messageData)

			if errJson != nil{
				
				go log.Println(errJson)
					
				callbackChan <- "REJECT"

				return

			}

			e.contentMatch(messageData)

		}else{

			SubscriberChannel <- bodyPacket

		}

		callbackChan <- "SUCCESS"	
	}
}

func (e *CreateProperties) contentMatch(messageData map[string]interface{}){

	var matchFound = true

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