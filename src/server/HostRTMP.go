package server

import(
	"pojo"
	"ChannelList"
	"server/rtmp"
	rtmpChild "server/rtmp/rtmp"
	"log"
)

func HostRTMP(configObj pojo.Config){

	defer ChannelList.Recover()

	ChannelList.ConfigRTMPObj = configObj

	if *ChannelList.ConfigRTMPObj.Server.RTMP.Host != "" && *ChannelList.ConfigRTMPObj.Server.RTMP.Port != ""{
		HostRTMPServer()
	}

}

func HostRTMPServer(){

	defer ChannelList.Recover()

	rtmp.LoadRTMPChannelsToMemory()

	ChannelList.WriteLog("Loading log files...")
	ChannelList.WriteLog("Starting RTMP server...")

	err := rtmpChild.ListenAndServe(*ChannelList.ConfigRTMPObj.Server.RTMP.Host+":"+*ChannelList.ConfigRTMPObj.Server.RTMP.Port)

	if err != nil{

		panic(err)

	}

	log.Println("Listening on " + *ChannelList.ConfigRTMPObj.Server.RTMP.Host + ":" + *ChannelList.ConfigRTMPObj.Server.RTMP.Port+"...")

}