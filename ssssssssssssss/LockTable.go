package tcp

/*
	This file contains all locking of channels using mutex to prevent race conditions when using multple goroutines
*/

import(
	"pojo"
	"ChannelList"
	"os"
	"runtime"
)

// Subscriber group struct declaration

type SubscriberGroup struct{

	Index int
	Packet pojo.PacketStruct

}

//######################################################### Persistent Channel Methods #############################################################################

// method to write cursor counter, locking with mutex

func writeSubscriberGrpOffset(index int, packetObject pojo.PacketStruct, byteArrayCursor []byte) bool{

	defer ChannelList.Recover()

	runtime.Gosched()

	ChannelList.TCPStorage[packetObject.ChannelName].SubscriberFileChannelLock.Lock()
	_, err := packetObject.SubscriberFD[index].WriteAt(byteArrayCursor, 0)
	ChannelList.TCPStorage[packetObject.ChannelName].SubscriberFileChannelLock.Unlock()

	if (err != nil){

		go ChannelList.WriteLog(err.Error())

		return false
	}

	return true
}

// method to close file descriptor

func closeSubscriberGrpFD(packetObject pojo.PacketStruct){

	defer ChannelList.Recover()

	runtime.Gosched()

	ChannelList.TCPStorage[packetObject.ChannelName].SubscriberFileChannelLock.Lock()
	defer ChannelList.TCPStorage[packetObject.ChannelName].SubscriberFileChannelLock.Unlock()

	for fileIndex := range packetObject.SubscriberFD{

		packetObject.SubscriberFD[fileIndex].Close()

	}
}

// creating file descriptor array to read from the log file

func createSubscriberGrpFD(ChannelName string) []*os.File{

	defer ChannelList.Recover()

	runtime.Gosched()

	ChannelList.TCPStorage[ChannelName].SubscriberFileChannelLock.Lock()
	fileFDArray := make([]*os.File, ChannelList.TCPStorage[ChannelName].PartitionCount)
	ChannelList.TCPStorage[ChannelName].SubscriberFileChannelLock.Unlock()

	return fileFDArray

}

// adding file descriptor object to array

func addSubscriberFD(index int, packetObject pojo.PacketStruct, fDes *os.File){

	defer ChannelList.Recover()

	runtime.Gosched()

	ChannelList.TCPStorage[packetObject.ChannelName].SubscriberFileChannelLock.Lock()
	packetObject.SubscriberFD[index] = fDes
	ChannelList.TCPStorage[packetObject.ChannelName].SubscriberFileChannelLock.Unlock()

}

// reading the status of the channelist list with channel name exists or not

func loadTCPChannelSubscriberList(channelName string, subscriberName string) bool{

	defer ChannelList.Recover()

	runtime.Gosched()

	ChannelList.TCPStorage[channelName].SubscriberChannelLock.RLock()
	status := ChannelList.TCPStorage[channelName].SubscriberList[subscriberName]
	ChannelList.TCPStorage[channelName].SubscriberChannelLock.RUnlock()

	return status
}	

//storing status into the subscriber list hash map

func storeTCPChannelSubscriberList(channelName string, subscriberName string, status bool){

	defer ChannelList.Recover()

	runtime.Gosched()

	ChannelList.TCPStorage[channelName].SubscriberChannelLock.Lock()
	ChannelList.TCPStorage[channelName].SubscriberList[subscriberName] = status
	ChannelList.TCPStorage[channelName].SubscriberChannelLock.Unlock()

}

// deleting subscriber name from subscriber list when the subscriber disconnects

func deleteTCPChannelSubscriberList(channelName string, subscriberName string){

	defer ChannelList.Recover()

	runtime.Gosched()

	ChannelList.TCPStorage[channelName].SubscriberChannelLock.Lock()
	delete(ChannelList.TCPStorage[channelName].SubscriberList, subscriberName)	
	ChannelList.TCPStorage[channelName].SubscriberChannelLock.Unlock()
}

// renewing subscriber list of any channel group

func renewSub(channelName string, groupName string){

	defer ChannelList.Recover()

	runtime.Gosched()

	ChannelList.TCPStorage[channelName].ChannelLock.Lock()
	defer ChannelList.TCPStorage[channelName].ChannelLock.Unlock()

	var newList []*pojo.PacketStruct

	ChannelList.TCPStorage[channelName].Group[groupName] = newList

}

// adding new client to subscriber channel group

func addNewClientToGrp(channelName string , groupName string, packetObject pojo.PacketStruct){

	defer ChannelList.Recover()

	runtime.Gosched()

	ChannelList.TCPStorage[channelName].ChannelLock.Lock()
	ChannelList.TCPStorage[channelName].Group[groupName] = append(ChannelList.TCPStorage[channelName].Group[groupName], &packetObject)
	ChannelList.TCPStorage[channelName].ChannelLock.Unlock()
	
}

// removeing subscriber from chanel group

func removeGroupMember(channelName string , groupName string, consumerName string){

	defer ChannelList.Recover()

	runtime.Gosched()

	ChannelList.TCPStorage[channelName].ChannelLock.Lock()
	defer ChannelList.TCPStorage[channelName].ChannelLock.Unlock()

	for index := range ChannelList.TCPStorage[channelName].Group[groupName]{

		groupMap := ChannelList.TCPStorage[channelName].Group[groupName][index]

		groupConsumerName := groupMap.ChannelName + groupMap.SubscriberName + groupMap.GroupName

		if groupConsumerName == consumerName{

			ChannelList.TCPStorage[channelName].Group[groupName] = append(ChannelList.TCPStorage[channelName].Group[groupName][:index], ChannelList.TCPStorage[channelName].Group[groupName][index+1:]...)

			break
		}
	}

}

// removing the group from the hashmap

func removeGroupMap(channelName string , groupName string, groupId *int){

	defer ChannelList.Recover()

	runtime.Gosched()

	ChannelList.TCPStorage[channelName].ChannelLock.Lock()
	defer ChannelList.TCPStorage[channelName].ChannelLock.Unlock()

	if len(ChannelList.TCPStorage[channelName].Group[groupName]) > 0{

		ChannelList.TCPStorage[channelName].Group[groupName] = append(ChannelList.TCPStorage[channelName].Group[groupName][:*groupId], ChannelList.TCPStorage[channelName].Group[groupName][*groupId+1:]...)
	}

}

// getting the value groupId and subscriber packet object

func getValue(channelName string , groupName string, groupId *int, index int) (*pojo.PacketStruct, int){

	defer ChannelList.Recover()

	runtime.Gosched()

	ChannelList.TCPStorage[channelName].ChannelLock.RLock()
	defer ChannelList.TCPStorage[channelName].ChannelLock.RUnlock()

	// log.Println(ChannelList.TCPStorage[channelName].Group[groupName])

	groupLen := len(ChannelList.TCPStorage[channelName].Group[groupName])

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

// getting the total size of the channel group

func getChannelGrpMapLen(channelName string, groupName string) int{

	defer ChannelList.Recover()

	runtime.Gosched()

	ChannelList.TCPStorage[channelName].ChannelLock.RLock()
	groupLen := len(ChannelList.TCPStorage[channelName].Group[groupName])
	ChannelList.TCPStorage[channelName].ChannelLock.RUnlock()

	return groupLen
}
	
// ################################################## Inmemory Channel Methods ##########################################################################

// adding new client to inmemory channel subscriber hashmap 

func appendNewClientInmemory(channelName string, subscriberMapName string, packetObject *pojo.PacketStruct){

	defer ChannelList.Recover()

	runtime.Gosched()

	ChannelList.TCPStorage[channelName].ChannelLock.Lock()
	ChannelList.TCPSocketDetails[channelName][subscriberMapName] = packetObject
	ChannelList.TCPStorage[channelName].ChannelLock.Unlock()

}

// getting client list of inmemory subscriber list

func getClientListInmemory(channelName string) map[string]*pojo.PacketStruct{

	defer ChannelList.Recover()

	runtime.Gosched()

	ChannelList.TCPStorage[channelName].ChannelLock.RLock()
	channelInmemoryList := ChannelList.TCPSocketDetails[channelName]
	ChannelList.TCPStorage[channelName].ChannelLock.RUnlock()

	return channelInmemoryList
}

// deleting the user from the hashmap subscriber channelList

func deleteInmemoryChannelList(channelName string, subscriberMapName string){

	defer ChannelList.Recover()

	runtime.Gosched()

	ChannelList.TCPStorage[channelName].ChannelLock.Lock()
	delete(ChannelList.TCPSocketDetails[channelName], subscriberMapName)
	ChannelList.TCPStorage[channelName].ChannelLock.Unlock()
}

// checking if the client exists in the inmemory subscriber channel list

func findInmemorySocketListLength(channelName string, key string) (bool, *pojo.PacketStruct){

	defer ChannelList.Recover()

	runtime.Gosched()

	ChannelList.TCPStorage[channelName].ChannelLock.RLock()
	defer ChannelList.TCPStorage[channelName].ChannelLock.RUnlock()

	var cb bool

	var sockClient *pojo.PacketStruct

	if val, ok := ChannelList.TCPSocketDetails[channelName][key]; ok {

		sockClient = val

		cb = ok
	}

	return cb, sockClient
}
