package server

import(
	"fmt"
	"io/ioutil"
	"path/filepath"
	"encoding/json"
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

				var bucketData  = make(chan string)

				var channelName = channelMap["channelName"].(string)

				TCPStorage[channelName] = make(map[string]interface{})
				TCPStorage[channelName]["path"] = channelMap["path"]
				TCPStorage[channelName]["writeInterval"] = channelMap["writeInterval"]
				TCPStorage[channelName]["bucketData"] = bucketData

			}	

        }
    }
}

func LoadUDPChannelsToMemory(){

    
}