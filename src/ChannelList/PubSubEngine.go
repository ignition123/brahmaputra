package ChannelList

import(
	"objects"
	"time"
	_"sync"
	_"log"
)

func HandleSubscriberMessages(channelName string, SubscriberObj *objects.Subscribers){

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

			case clientObj, channelStat := <-SubscriberObj.UnRegister:

				if channelStat{

					select{

						case clientObj.Channel <- nil:
						case <-time.After(1 * time.Millisecond):

					}

					if clientObj.GroupMapName != ""{

						UnRegisterGroup(clientObj, SubscriberObj)

					}

					delete(SubscriberObj.Clients, clientObj)

				}

			case groupName, channelStat := <-SubscriberObj.GroupUnRegister:

				if channelStat{

					DeleteGroup(groupName, SubscriberObj)

				}

			case message, channelStat := <-SubscriberObj.BroadCast:

				if channelStat{

					for clientObj, _ :=  range SubscriberObj.Clients{

						select{

							case clientObj.Channel <- message:
							case <-time.After(5 * time.Second):
						}

					}


				}

			case <-time.After(1 * time.Second):
		}

	}

}