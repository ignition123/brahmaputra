package server

import(
	"fmt"
	"io/ioutil"
	"path/filepath"
	"encoding/json"
	"pojo"
)

func LoadTCPChannelsToMemory(){

    files, err := ioutil.ReadDir("./storage/")

    if err != nil {
        WriteLog(err.Error())
        return
    }

    for _, file := range files{

    	extension := filepath.Ext(file.Name())

        if extension == ".json"{

        	data, err := ioutil.ReadFile("./storage/"+file.Name())

			if err != nil{
				fmt.Println(err.Error())
				WriteLog(err.Error())
				break
			}

			channelMap := make(map[string]interface{})

			err = json.Unmarshal(data, &channelMap)

			if err != nil{
				fmt.Println(err.Error())
				WriteLog(err.Error())
				break
			}

			if channelMap["type"] == "channel" && channelMap["channelType"] == "tcp"{

				var bucketData  = make(chan map[string]interface{})

				var channelName = channelMap["channelName"].(string)

				var channelObject = &pojo.ChannelStruct{
					Path: channelMap["path"].(string),
					WriteInterval:int32(channelMap["writeInterval"].(float64)),
					BucketData: bucketData,
				}

				TCPStorage[channelName] = channelObject

			}	

        }
    }
}

func LoadUDPChannelsToMemory(){

    
}