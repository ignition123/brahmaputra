package server

import(
	"io/ioutil"
	"path/filepath"
	"encoding/json"
	"pojo"
	"os"
	"ChannelList"
	_"log"
)

func LoadTCPChannelsToMemory(){

	defer ChannelList.Recover()

    files, err := ioutil.ReadDir(*ChannelList.ConfigTCPObj.ChannelConfigFiles)

    if err != nil {
        ChannelList.WriteLog(err.Error())
        return
    }

    for _, file := range files{

    	extension := filepath.Ext(file.Name())

        if extension == ".json"{

        	data, err := ioutil.ReadFile(*ChannelList.ConfigTCPObj.ChannelConfigFiles+"/"+file.Name())

			if err != nil{
				ChannelList.WriteLog(err.Error())
				break
			}

			channelMap := make(map[string]interface{})

			err = json.Unmarshal(data, &channelMap)

			if err != nil{
				ChannelList.WriteLog(err.Error())
				break
			}

			if channelMap["type"] == "channel" && channelMap["channelType"] == "tcp"{

				var worker = int(channelMap["worker"].(float64))

				var bucketData  = make([]chan *pojo.PacketStruct, worker)

				for i := range bucketData {
				   bucketData[i] = make(chan *pojo.PacketStruct, *ChannelList.ConfigTCPObj.Server.TCP.BufferRead)
				}

				var channelName = channelMap["channelName"].(string)

				var channelObject = &pojo.ChannelStruct{
					Offset:int64(0),
					Worker: worker,
					BucketData: bucketData,
					WriteCallback:make(chan bool),
					ChannelStorageType: channelMap["channelStorageType"].(string),
				}

				if *ChannelList.ConfigTCPObj.Storage.File.Active && channelName != "heart_beat"{

					if channelObject.ChannelStorageType == "persistent"{
						channelObject = openDataFile("tcp", channelObject, channelMap)
					}
				}

				ChannelList.TCPStorage[channelName] = channelObject

			}	

        }
    }
}

func openDataFile(protocol string, channelObject *pojo.ChannelStruct, channelMap map[string]interface{}) *pojo.ChannelStruct{

	defer ChannelList.Recover()

	if protocol == "tcp"{

		f, err := os.OpenFile(channelMap["path"].(string),
			os.O_APPEND|os.O_WRONLY, 0700)

		if err != nil {
			ChannelList.WriteLog(err.Error())
			return channelObject
		}

		channelObject.FD = f
		channelObject.Path = channelMap["path"].(string)
		
	}else if protocol == "udp"{


	}

	return channelObject
}

func LoadUDPChannelsToMemory(){

    
}