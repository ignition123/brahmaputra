package ChannelList

import(
	"pojo"
	_"time"
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

					if clientObj.GroupMapName != ""{

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

		}

	}

}