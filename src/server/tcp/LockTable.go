package tcp

import(
	"pojo"
	"ChannelList"
	"os"
)

type SubscriberGroup struct{

	Index int
	Packet pojo.PacketStruct

}


func WriteSubscriberGrpOffset(index int, packetObject pojo.PacketStruct, byteArrayCursor []byte) bool{

	defer ChannelList.Recover()

	ChannelList.TCPStorage[packetObject.ChannelName].SubscriberFileChannelLock.Lock()
	_, err := packetObject.SubscriberFD[index].WriteAt(byteArrayCursor, 0)
	ChannelList.TCPStorage[packetObject.ChannelName].SubscriberFileChannelLock.Unlock()

	if (err != nil){

		go ChannelList.WriteLog(err.Error())

		return false
	}

	return true
}

func CloseSubscriberGrpFD(packetObject pojo.PacketStruct){

	defer ChannelList.Recover()

	ChannelList.TCPStorage[packetObject.ChannelName].SubscriberFileChannelLock.Lock()
	defer ChannelList.TCPStorage[packetObject.ChannelName].SubscriberFileChannelLock.Unlock()

	for fileIndex := range packetObject.SubscriberFD{

		packetObject.SubscriberFD[fileIndex].Close()

	}
}

func CreateSubscriberGrpFD(ChannelName string) []*os.File{

	defer ChannelList.Recover()

	ChannelList.TCPStorage[ChannelName].SubscriberFileChannelLock.Lock()
	var fileFDArray = make([]*os.File, ChannelList.TCPStorage[ChannelName].PartitionCount)
	ChannelList.TCPStorage[ChannelName].SubscriberFileChannelLock.Unlock()

	return fileFDArray

}

func AddSubscriberFD(index int, packetObject pojo.PacketStruct, fDes *os.File){

	defer ChannelList.Recover()

	ChannelList.TCPStorage[packetObject.ChannelName].SubscriberFileChannelLock.Lock()
	packetObject.SubscriberFD[index] = fDes
	ChannelList.TCPStorage[packetObject.ChannelName].SubscriberFileChannelLock.Unlock()

}

func LoadTCPChannelSubscriberList(channelName string, subscriberName string) bool{

	defer ChannelList.Recover()

	ChannelList.TCPStorage[channelName].SubscriberChannelLock.RLock()
	var status = ChannelList.TCPStorage[channelName].SubscriberList[subscriberName]
	ChannelList.TCPStorage[channelName].SubscriberChannelLock.RUnlock()

	return status
}	

func StoreTCPChannelSubscriberList(channelName string, subscriberName string, status bool){

	defer ChannelList.Recover()

	ChannelList.TCPStorage[channelName].SubscriberChannelLock.Lock()
	ChannelList.TCPStorage[channelName].SubscriberList[subscriberName] = status
	ChannelList.TCPStorage[channelName].SubscriberChannelLock.Unlock()

}

func DeleteTCPChannelSubscriberList(channelName string, subscriberName string){

	defer ChannelList.Recover()

	ChannelList.TCPStorage[channelName].SubscriberChannelLock.Lock()
	delete(ChannelList.TCPStorage[channelName].SubscriberList, subscriberName)	
	ChannelList.TCPStorage[channelName].SubscriberChannelLock.Unlock()
}


func RenewSub(channelName string, groupName string){

	defer ChannelList.Recover()

	ChannelList.TCPStorage[channelName].ChannelLock.Lock()
	defer ChannelList.TCPStorage[channelName].ChannelLock.Unlock()

	var newList []*pojo.PacketStruct

	ChannelList.TCPStorage[channelName].Group[groupName] = newList

}

func AddNewClientToGrp(channelName string , groupName string, packetObject pojo.PacketStruct){

	defer ChannelList.Recover()

	ChannelList.TCPStorage[channelName].ChannelLock.Lock()
	ChannelList.TCPStorage[channelName].Group[groupName] = append(ChannelList.TCPStorage[channelName].Group[groupName], &packetObject)
	ChannelList.TCPStorage[channelName].ChannelLock.Unlock()
	
}

func RemoveGroupMember(channelName string , groupName string, consumerName string){

	defer ChannelList.Recover()

	ChannelList.TCPStorage[channelName].ChannelLock.Lock()
	defer ChannelList.TCPStorage[channelName].ChannelLock.Unlock()

	for index := range ChannelList.TCPStorage[channelName].Group[groupName]{

		var groupMap = ChannelList.TCPStorage[channelName].Group[groupName][index]

		var groupConsumerName = groupMap.ChannelName + groupMap.SubscriberName + groupMap.GroupName

		if groupConsumerName == consumerName{

			ChannelList.TCPStorage[channelName].Group[groupName] = append(ChannelList.TCPStorage[channelName].Group[groupName][:index], ChannelList.TCPStorage[channelName].Group[groupName][index+1:]...)

			break
		}
	}

}

func RemoveGroupMap(channelName string , groupName string, groupId *int){

	defer ChannelList.Recover()

	ChannelList.TCPStorage[channelName].ChannelLock.Lock()
	defer ChannelList.TCPStorage[channelName].ChannelLock.Unlock()

	if len(ChannelList.TCPStorage[channelName].Group[groupName]) > 0{

		ChannelList.TCPStorage[channelName].Group[groupName] = append(ChannelList.TCPStorage[channelName].Group[groupName][:*groupId], ChannelList.TCPStorage[channelName].Group[groupName][*groupId+1:]...)
	}

}

func GetValue(channelName string , groupName string, groupId *int, index int) (*pojo.PacketStruct, int){

	defer ChannelList.Recover()

	ChannelList.TCPStorage[channelName].ChannelLock.RLock()
	defer ChannelList.TCPStorage[channelName].ChannelLock.RUnlock()

	// log.Println(ChannelList.TCPStorage[channelName].Group[groupName])

	var groupLen = len(ChannelList.TCPStorage[channelName].Group[groupName])

	if groupLen <= 0{

		return nil, -1

	}

	*groupId = index % groupLen

	if *groupId >= groupLen{

		if groupLen == 0{

			return nil, -1

		}else{

			return ChannelList.TCPStorage[channelName].Group[groupName][0], 0

		}

	}else{

		return ChannelList.TCPStorage[channelName].Group[groupName][*groupId], *groupId
		
	}

	return nil, -1
}

func GetChannelGrpMapLen(channelName string, groupName string) int{

	defer ChannelList.Recover()

	ChannelList.TCPStorage[channelName].ChannelLock.RLock()
	var groupLen = len(ChannelList.TCPStorage[channelName].Group[groupName])
	ChannelList.TCPStorage[channelName].ChannelLock.RUnlock()

	return groupLen
}
