package ChannelList

import(
	"objects"
	"time"
)



// global object to store channel config inmemory and packet object + producer and subscriber details

var ConfigTCPObj objects.Config
var ConfigUDPObj objects.Config

func CreateSubscriberChannels(channelName string, channelObject *objects.ChannelStruct){

	defer Recover()

	objects.SubscriberObj[channelName] = &objects.Subscribers{
		Channel: channelObject,
		GroupUnRegister: make(chan string, *ConfigTCPObj.Server.TCP.BufferRead),
		Register: make(chan *objects.ClientObject, *ConfigTCPObj.Server.TCP.BufferRead),
		UnRegister: make(chan *objects.ClientObject, *ConfigTCPObj.Server.TCP.BufferRead),
		BroadCast: make(chan *objects.PublishMsg, *ConfigTCPObj.Server.TCP.BufferRead),
		Clients: make(map[*objects.ClientObject] bool),
		CurrentTime: time.Now(),
	}

	go HandleSubscriberMessages(channelName, objects.SubscriberObj[channelName])
}