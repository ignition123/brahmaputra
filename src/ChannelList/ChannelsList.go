package ChannelList

import(
	"pojo"
	"time"
)



// global object to store channel config inmemory and packet object + producer and subscriber details

var ConfigTCPObj pojo.Config
var ConfigUDPObj pojo.Config

func CreateSubscriberChannels(channelName string, channelObject *pojo.ChannelStruct){

	defer Recover()

	pojo.SubscriberObj[channelName] = &pojo.Subscribers{
		Channel: channelObject,
		GroupUnRegister: make(chan string, *ConfigTCPObj.Server.TCP.BufferRead),
		Register: make(chan *pojo.ClientObject, *ConfigTCPObj.Server.TCP.BufferRead),
		UnRegister: make(chan *pojo.ClientObject, *ConfigTCPObj.Server.TCP.BufferRead),
		BroadCast: make(chan *pojo.PacketStruct, *ConfigTCPObj.Server.TCP.BufferRead),
		Clients: make(map[*pojo.ClientObject] bool),
		CurrentTime: time.Now(),
	}

	go HandleSubscriberMessages(channelName, pojo.SubscriberObj[channelName])
}