package udp

import(
	"pojo"
	"ChannelList"
	"os"
)

type SubscriberGroup struct{

	Index int
	Packet pojo.UDPPacketStruct

}


func WriteSubscriberGrpOffset(index int, packetObject pojo.UDPPacketStruct, byteArrayCursor []byte) bool{

	defer ChannelList.Recover()

	ChannelList.UDPStorage[packetObject.ChannelName].SubscriberFileChannelLock.Lock()
	_, err := packetObject.SubscriberFD[index].WriteAt(byteArrayCursor, 0)
	ChannelList.UDPStorage[packetObject.ChannelName].SubscriberFileChannelLock.Unlock()

	if (err != nil){

		go ChannelList.WriteLog(err.Error())

		return false
	}

	return true
}

func CloseSubscriberGrpFD(packetObject pojo.UDPPacketStruct){

	defer ChannelList.Recover()

	ChannelList.UDPStorage[packetObject.ChannelName].SubscriberFileChannelLock.Lock()
	defer ChannelList.UDPStorage[packetObject.ChannelName].SubscriberFileChannelLock.Unlock()

	for fileIndex := range packetObject.SubscriberFD{

		packetObject.SubscriberFD[fileIndex].Close()

	}
}

func CreateSubscriberGrpFD(ChannelName string) []*os.File{

	defer ChannelList.Recover()

	ChannelList.UDPStorage[ChannelName].SubscriberFileChannelLock.Lock()
	var fileFDArray = make([]*os.File, ChannelList.UDPStorage[ChannelName].PartitionCount)
	ChannelList.UDPStorage[ChannelName].SubscriberFileChannelLock.Unlock()

	return fileFDArray

}

func AddSubscriberFD(index int, packetObject pojo.UDPPacketStruct, fDes *os.File){

	defer ChannelList.Recover()

	ChannelList.UDPStorage[packetObject.ChannelName].SubscriberFileChannelLock.Lock()
	packetObject.SubscriberFD[index] = fDes
	ChannelList.UDPStorage[packetObject.ChannelName].SubscriberFileChannelLock.Unlock()

}

func LoadUDPChannelSubscriberList(channelName string, subscriberName string) bool{

	defer ChannelList.Recover()

	ChannelList.UDPStorage[channelName].SubscriberChannelLock.RLock()
	var status = ChannelList.UDPStorage[channelName].SubscriberList[subscriberName]
	ChannelList.UDPStorage[channelName].SubscriberChannelLock.RUnlock()

	return status
}	

func StoreUDPChannelSubscriberList(channelName string, subscriberName string, status bool){

	defer ChannelList.Recover()

	ChannelList.UDPStorage[channelName].SubscriberChannelLock.Lock()
	ChannelList.UDPStorage[channelName].SubscriberList[subscriberName] = status
	ChannelList.UDPStorage[channelName].SubscriberChannelLock.Unlock()

}

func DeleteUDPChannelSubscriberList(channelName string, subscriberName string){

	defer ChannelList.Recover()

	ChannelList.UDPStorage[channelName].SubscriberChannelLock.Lock()
	delete(ChannelList.UDPStorage[channelName].SubscriberList, subscriberName)	
	ChannelList.UDPStorage[channelName].SubscriberChannelLock.Unlock()
}


func RenewSub(channelName string, groupName string){

	defer ChannelList.Recover()

	ChannelList.UDPStorage[channelName].ChannelLock.Lock()
	defer ChannelList.UDPStorage[channelName].ChannelLock.Unlock()

	var newList []*pojo.UDPPacketStruct

	ChannelList.UDPStorage[channelName].Group[groupName] = newList

}

func AddNewClientToGrp(channelName string , groupName string, packetObject pojo.UDPPacketStruct){

	defer ChannelList.Recover()

	ChannelList.UDPStorage[channelName].ChannelLock.Lock()
	ChannelList.UDPStorage[channelName].Group[groupName] = append(ChannelList.UDPStorage[channelName].Group[groupName], &packetObject)
	ChannelList.UDPStorage[channelName].ChannelLock.Unlock()
	
}

func RemoveGroupMember(channelName string , groupName string, consumerName string){

	defer ChannelList.Recover()

	ChannelList.UDPStorage[channelName].ChannelLock.Lock()
	defer ChannelList.UDPStorage[channelName].ChannelLock.Unlock()

	for index := range ChannelList.UDPStorage[channelName].Group[groupName]{

		var groupMap = ChannelList.UDPStorage[channelName].Group[groupName][index]

		var groupConsumerName = groupMap.ChannelName + groupMap.SubscriberName + groupMap.GroupName

		if groupConsumerName == consumerName{

			ChannelList.UDPStorage[channelName].Group[groupName] = append(ChannelList.UDPStorage[channelName].Group[groupName][:index], ChannelList.UDPStorage[channelName].Group[groupName][index+1:]...)

			break
		}
	}

}

func RemoveGroupMap(channelName string , groupName string, groupId *int){

	defer ChannelList.Recover()

	ChannelList.UDPStorage[channelName].ChannelLock.Lock()
	defer ChannelList.UDPStorage[channelName].ChannelLock.Unlock()

	if len(ChannelList.UDPStorage[channelName].Group[groupName]) > 0{

		ChannelList.UDPStorage[channelName].Group[groupName] = append(ChannelList.UDPStorage[channelName].Group[groupName][:*groupId], ChannelList.UDPStorage[channelName].Group[groupName][*groupId+1:]...)
	}

}

func GetValue(channelName string , groupName string, groupId *int, index int) (*pojo.UDPPacketStruct, int){

	defer ChannelList.Recover()

	ChannelList.UDPStorage[channelName].ChannelLock.RLock()
	defer ChannelList.UDPStorage[channelName].ChannelLock.RUnlock()

	// log.Println(ChannelList.UDPStorage[channelName].Group[groupName])

	var groupLen = len(ChannelList.UDPStorage[channelName].Group[groupName])

	if groupLen <= 0{

		return nil, -1

	}

	*groupId = index % groupLen

	if *groupId >= groupLen{

		if groupLen == 0{

			return nil, -1

		}else{

			return ChannelList.UDPStorage[channelName].Group[groupName][0], 0

		}

	}else{

		return ChannelList.UDPStorage[channelName].Group[groupName][*groupId], *groupId
		
	}

	return nil, -1
}

func GetChannelGrpMapLen(channelName string, groupName string) int{

	defer ChannelList.Recover()

	ChannelList.UDPStorage[channelName].ChannelLock.RLock()
	var groupLen = len(ChannelList.UDPStorage[channelName].Group[groupName])
	ChannelList.UDPStorage[channelName].ChannelLock.RUnlock()

	return groupLen
}
