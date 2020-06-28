package ChannelList

import(
	"pojo"
	"time"
	_"sync"
	_"log"
)

func HandleSubscriberMessages(channelName string, SubscriberObj *pojo.Subscribers){

	defer Recover()

	for{

		select{

			case clientObj, channelStat := <-SubscriberObj.Register:

				if channelStat{

					SubscriberObj.Clients[clientObj] = true

					if clientObj.GroupMapName != ""{

						RegisterGroup(channelName, clientObj, SubscriberObj)

					}

				}

			break

			case clientObj, channelStat := <-SubscriberObj.UnRegister:

				if channelStat{
					
					clientObj.Channel <- nil

					groupLen := len(SubscriberObj.Groups[clientObj.GroupMapName])

					if groupLen > 0{

						UnRegisterGroup(clientObj, SubscriberObj)

					}

					delete(SubscriberObj.Clients, clientObj)

				}

			break

			case groupName, channelStat := <-SubscriberObj.GroupUnRegister:

				if channelStat{

					DeleteGroup(groupName, SubscriberObj)

				}
			break

			case message, channelStat := <-SubscriberObj.BroadCast:

				if channelStat{

					for clientObj, _ :=  range SubscriberObj.Clients{

						clientObj.Channel <- message

					}

				}

			break
			
			case <-time.After(10 * time.Second):
		 	break 

		}

	}

}

func RegisterGroup(channelName string, clientObj *pojo.ClientObject, SubscriberObj *pojo.Subscribers){

	defer Recover()

	groupLen := len(SubscriberObj.Groups[clientObj.GroupMapName])

	// if group length == channel partition count then error

	if groupLen == SubscriberObj.Channel.PartitionCount{

		ThroughClientError(clientObj.Conn, SUBSCRIBER_FULL)

		return

	}

	CreateGroup(channelName, clientObj, SubscriberObj)

}
