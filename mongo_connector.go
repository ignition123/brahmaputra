if *ChannelList.ConfigTCPObj.Storage.Mongodb.Active{

	var byteLen = len(jsonData)

	go WriteMongodbData(nanoEpoch, messageMap["AgentName"].(string), jsonData, channelName, byteLen)

	select {

		case message, ok := <-ChannelList.TCPStorage[channelName].WriteMongoCallback:	
			if ok{
				
				if !message{
					parseChan <- false
					return
				}
			}		
			break
	}
} 

func WriteMongodbData(_id int64, agentName string, jsonData []byte, channelName string, byteLen int){

	defer ChannelList.Recover()

	var packetBuffer bytes.Buffer

	buff := make([]byte, 4)

	binary.LittleEndian.PutUint32(buff, uint32(byteLen))

	packetBuffer.Write(buff)

	packetBuffer.Write(jsonData)

	var oneDoc = make(map[string]interface{})

	oneDoc["offsetID"] = _id
	oneDoc["AgentName"] = agentName
	oneDoc["cluster"] = *ChannelList.ConfigTCPObj.Server.TCP.ClusterName
	oneDoc["data"] = packetBuffer.Bytes()

	var status, _ = MongoConnection.InsertOne(channelName, oneDoc)

	if !status{
		ChannelList.TCPStorage[channelName].WriteMongoCallback <- false
	}else{
		ChannelList.TCPStorage[channelName].WriteMongoCallback <- true
	}
}

WriteMongoCallback chan bool

if !ConnectStorage(){
		ChannelList.WriteLog("Unable to connect to storage...")
		return
	}

	func ConnectStorage() bool{

	defer ChannelList.Recover()
	
	if *ChannelList.ConfigTCPObj.Storage.Mongodb.Active{
		
		if(!MongoConnection.Connect()){
			
			log.Println("Failed to connect Mongodb")

			return false
		}

		if !MongoConnection.SetupCollection(){
			return false
		}

	}
	
	return true

}

"MongoConnection"

WriteMongoCallback:make(chan bool),