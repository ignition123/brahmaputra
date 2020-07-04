package ChannelList

/*
	This file contains all locking of channels using mutex to prevent race conditions when using multple goroutines
*/

import(
	"objects"
	"os"
	"sync"
)


var channelLock = &sync.RWMutex{}

//######################################################### Persistent Channel Methods #############################################################################

// method to write cursor counter, locking with mutex

func WriteSubscriberOffset(index int, packetObject *objects.PacketStruct, clientObj *objects.ClientObject, byteArrayCursor []byte) bool{

	defer Recover()

	_, err := packetObject.SubscriberFD[index].WriteAt(byteArrayCursor, 0)

	if (err != nil){

		go WriteLog(err.Error())

		return false
	}

	return true
}


// creating file descriptor array to read from the log file

func CreateSubscriberGrpFD(ChannelName string) []*os.File{

	defer Recover()

	channelLock.RLock()
	fileFDArray := make([]*os.File, objects.SubscriberObj[ChannelName].Channel.PartitionCount)
	channelLock.RUnlock()

	return fileFDArray

}

// method to write cursor counter, locking with mutex

func WriteSubscriberGrpOffset(index int, packetObject *objects.PacketStruct, byteArrayCursor []byte) bool{

	defer Recover()

	_, err := packetObject.SubscriberFD[index].WriteAt(byteArrayCursor, 0)

	if (err != nil){

		go WriteLog(err.Error())

		return false
	}

	return true
}

// getting the total size of the channel group

func GetChannelGrpMapLen(channelName string, groupName string) int{

	defer Recover()

	var groupLen int

	channelLock.RLock()
	_, ok := objects.SubscriberObj[channelName].Groups[groupName]
	channelLock.RUnlock()

	if !ok{

		groupLen = 0

	}else{

		channelLock.RLock()
		groupLen = len(objects.SubscriberObj[channelName].Groups[groupName])
		channelLock.RUnlock()
	}
	
	return groupLen
}

// create new Group

func CreateGroup(channelName string, clientObj *objects.ClientObject, SubscriberObj *objects.Subscribers){

	defer Recover()

	channelLock.Lock()
	defer channelLock.Unlock()

	_, ok := SubscriberObj.Groups[clientObj.GroupMapName]

	if !ok{
		SubscriberObj.Groups = make(map[string] []*objects.ClientObject)
	}
	
	SubscriberObj.Groups[clientObj.GroupMapName] = append(SubscriberObj.Groups[clientObj.GroupMapName], clientObj) //
}

// get clientObject

func GetClientObject(channelName string, groupName string, index int) (*objects.ClientObject, int, int){

	defer Recover()

	var groupLen, groupId int

	channelLock.RLock()
	defer channelLock.RUnlock()

	_, ok := objects.SubscriberObj[channelName].Groups[groupName]

	if ok{

		groupLen = len(objects.SubscriberObj[channelName].Groups[groupName])

		if groupLen > 0{

			groupId = index % groupLen

		}

		if len(objects.SubscriberObj[channelName].Groups[groupName]) > 0{

			return objects.SubscriberObj[channelName].Groups[groupName][groupId], groupId, groupLen

		} 

	}

	return nil, 0, 0
}

// register in group

func RegisterGroup(channelName string, clientObj *objects.ClientObject, SubscriberObj *objects.Subscribers){

	defer Recover()

	groupLen := GetChannelGrpMapLen(channelName, clientObj.GroupMapName)

	// if group length == channel partition count then error

	if groupLen == SubscriberObj.Channel.PartitionCount{

		ThroughClientError(clientObj.Conn, SUBSCRIBER_FULL)

		return

	}

	CreateGroup(channelName, clientObj, SubscriberObj)

}


// UnregisterUser from group

func UnRegisterGroup(clientObj *objects.ClientObject, SubscriberObj *objects.Subscribers){

	defer Recover()

	channelLock.Lock()
	defer channelLock.Unlock()

	for index := range SubscriberObj.Groups[clientObj.GroupMapName]{

		if SubscriberObj.Groups[clientObj.GroupMapName][index] == clientObj{

			SubscriberObj.Groups[clientObj.GroupMapName] = append(SubscriberObj.Groups[clientObj.GroupMapName][:index], SubscriberObj.Groups[clientObj.GroupMapName][index+1:]...)

			break
		}

	}
}

// delete user from group

func DeleteGroup(groupName string, SubscriberObj *objects.Subscribers){

	defer Recover()

	for index := range SubscriberObj.Groups[groupName]{

		SubscriberObj.Groups[groupName][index].Conn.Close()

	}

	delete(SubscriberObj.Groups, groupName)

}