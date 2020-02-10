package server

import(
	"io/ioutil"
	"path/filepath"
	"encoding/json"
	"pojo"
	"os"
)

func LoadTCPChannelsToMemory(ConfigTCPObj pojo.Config){

    files, err := ioutil.ReadDir(*ConfigTCPObj.ChannelConfigFiles)

    if err != nil {
        WriteLog(err.Error())
        return
    }

    for _, file := range files{

    	extension := filepath.Ext(file.Name())

        if extension == ".json"{

        	data, err := ioutil.ReadFile(*ConfigTCPObj.ChannelConfigFiles+"/"+file.Name())

			if err != nil{
				WriteLog(err.Error())
				break
			}

			channelMap := make(map[string]interface{})

			err = json.Unmarshal(data, &channelMap)

			if err != nil{
				WriteLog(err.Error())
				break
			}

			if channelMap["type"] == "channel" && channelMap["channelType"] == "tcp"{

				var worker = int16(channelMap["worker"].(float64))

				var bucketData  = make([]chan map[string]interface{}, worker)

				for i := range bucketData {
				   bucketData[i] = make(chan map[string]interface{}, *ConfigTCPObj.Server.TCP.BufferRead)
				}

				var channelName = channelMap["channelName"].(string)

				var channelObject = &pojo.ChannelStruct{
					Path: channelMap["path"].(string),
					Offset:int64(0),
					Worker: worker,
					BucketData: bucketData,
				}

				if *ConfigTCPObj.Storage.File.Active && channelName != "heart_beat"{
					channelObject = openDataFile("tcp", channelObject, ConfigTCPObj, channelMap)
					channelObject = openTableFile("tcp", channelObject, ConfigTCPObj, channelMap)
				}

				TCPStorage[channelName] = channelObject

			}	

        }
    }
}

func openDataFile(protocol string, channelObject *pojo.ChannelStruct, ConfigTCPObj pojo.Config, channelMap map[string]interface{}) *pojo.ChannelStruct{

	if protocol == "tcp"{

		f, err := os.OpenFile(channelMap["path"].(string),
			os.O_APPEND|os.O_RDWR, 0700)

		if err != nil {
			WriteLog(err.Error())
			return channelObject
		}

		channelObject.FD = f

	}else if protocol == "udp"{


	}

	return channelObject
}

func openTableFile(protocol string, channelObject *pojo.ChannelStruct, ConfigTCPObj pojo.Config, channelMap map[string]interface{}) *pojo.ChannelStruct{

	if protocol == "tcp"{

		f, err := os.OpenFile(channelMap["table"].(string),
			os.O_APPEND|os.O_RDWR, 0700)

		if err != nil {
			WriteLog(err.Error())
			return channelObject
		}

		channelObject.TableFD = f

	}else if protocol == "udp"{


	}

	return channelObject
}

func LoadUDPChannelsToMemory(){

    
}