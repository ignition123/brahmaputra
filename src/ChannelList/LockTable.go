package ChannelList

/*
	This file contains all locking of channels using mutex to prevent race conditions when using multple goroutines
*/

import(
	"pojo"
	"os"
	"runtime"
)


//######################################################### Persistent Channel Methods #############################################################################

// method to write cursor counter, locking with mutex

func WriteSubscriberOffset(index int, packetObject *pojo.PacketStruct, clientObj *pojo.ClientObject, byteArrayCursor []byte) bool{

	defer Recover()

	runtime.Gosched()

	pojo.SubscriberObj[packetObject.ChannelName].Channel.SubscriberFileChannelLock.Lock()
	_, err := packetObject.SubscriberFD[index].WriteAt(byteArrayCursor, 0)
	pojo.SubscriberObj[packetObject.ChannelName].Channel.SubscriberFileChannelLock.Unlock()

	if (err != nil){

		go WriteLog(err.Error())

		return false
	}

	return true
}


// creating file descriptor array to read from the log file

func CreateSubscriberGrpFD(ChannelName string) []*os.File{

	defer Recover()

	runtime.Gosched()

	pojo.SubscriberObj[ChannelName].Channel.SubscriberFileChannelLock.Lock()
	fileFDArray := make([]*os.File, pojo.SubscriberObj[ChannelName].Channel.PartitionCount)
	pojo.SubscriberObj[ChannelName].Channel.SubscriberFileChannelLock.Unlock()

	return fileFDArray

}

// method to write cursor counter, locking with mutex

func WriteSubscriberGrpOffset(index int, packetObject *pojo.PacketStruct, byteArrayCursor []byte) bool{

	defer Recover()

	runtime.Gosched()

	pojo.SubscriberObj[packetObject.ChannelName].Channel.SubscriberFileChannelLock.Lock()
	_, err := packetObject.SubscriberFD[index].WriteAt(byteArrayCursor, 0)
	pojo.SubscriberObj[packetObject.ChannelName].Channel.SubscriberFileChannelLock.Unlock()

	if (err != nil){

		go WriteLog(err.Error())

		return false
	}

	return true
}

// getting the total size of the channel group

func GetChannelGrpMapLen(channelName string, groupName string) int{

	defer Recover()

	runtime.Gosched()

	var groupLen int

	pojo.SubscriberObj[channelName].Channel.SubscriberFileChannelLock.RLock()
	defer pojo.SubscriberObj[channelName].Channel.SubscriberFileChannelLock.RUnlock()

	_, ok := pojo.SubscriberObj[channelName].Groups[groupName]

	if !ok{

		groupLen = 0

	}else{

		groupLen = len(pojo.SubscriberObj[channelName].Groups[groupName])
	}
	
	return groupLen
}

// create new Group

func CreateGroup(channelName string, clientObj *pojo.ClientObject, SubscriberObj *pojo.Subscribers){

	defer Recover()

	runtime.Gosched()

	SubscriberObj.Channel.SubscriberFileChannelLock.Lock()
	defer SubscriberObj.Channel.SubscriberFileChannelLock.Unlock()

	_, ok := SubscriberObj.Groups[clientObj.GroupMapName]

	if !ok{
		SubscriberObj.Groups = make(map[string] []*pojo.ClientObject)
	}
	
	SubscriberObj.Groups[clientObj.GroupMapName] = append(SubscriberObj.Groups[clientObj.GroupMapName], clientObj) //
}

// get clientObject

func GetClientObject(channelName string, groupName string, index int) (*pojo.ClientObject, int, int){

	defer Recover()

	runtime.Gosched()

	pojo.SubscriberObj[channelName].Channel.SubscriberFileChannelLock.Lock()

	defer pojo.SubscriberObj[channelName].Channel.SubscriberFileChannelLock.Unlock()

	var groupLen, groupId int

	_, ok := pojo.SubscriberObj[channelName].Groups[groupName]

	if ok{

		groupLen = len(pojo.SubscriberObj[channelName].Groups[groupName])

		if groupLen > 0{

			groupId = index % groupLen

		}

		if len(pojo.SubscriberObj[channelName].Groups[groupName]) > 0{

			return pojo.SubscriberObj[channelName].Groups[groupName][groupId], groupId, groupLen

		} 

	}

	return nil, 0, 0
}

// register in group

func RegisterGroup(channelName string, clientObj *pojo.ClientObject, SubscriberObj *pojo.Subscribers){

	defer Recover()

	runtime.Gosched()

	groupLen := GetChannelGrpMapLen(channelName, clientObj.GroupMapName)

	// if group length == channel partition count then error

	if groupLen == SubscriberObj.Channel.PartitionCount{

		ThroughClientError(clientObj.Conn, SUBSCRIBER_FULL)

		return

	}

	CreateGroup(channelName, clientObj, SubscriberObj)

}


// UnregisterUser from group

func UnRegisterGroup(clientObj *pojo.ClientObject, SubscriberObj *pojo.Subscribers){

	defer Recover()

	runtime.Gosched()

	SubscriberObj.Channel.SubscriberFileChannelLock.Lock()
	defer SubscriberObj.Channel.SubscriberFileChannelLock.Unlock()

	for index := range SubscriberObj.Groups[clientObj.GroupMapName]{

		if SubscriberObj.Groups[clientObj.GroupMapName][index] == clientObj{

			SubscriberObj.Groups[clientObj.GroupMapName] = append(SubscriberObj.Groups[clientObj.GroupMapName][:index], SubscriberObj.Groups[clientObj.GroupMapName][index+1:]...)

			break
		}

	}
}

// delete user from group

func DeleteGroup(groupName string, SubscriberObj *pojo.Subscribers){

	defer Recover()

	runtime.Gosched()

	for index := range SubscriberObj.Groups[groupName]{

		SubscriberObj.Groups[groupName][index].Conn.Close()

	}

	delete(SubscriberObj.Groups, groupName)

}