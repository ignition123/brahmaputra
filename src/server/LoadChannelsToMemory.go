package server

import(
	"io/ioutil"
	"path/filepath"
	"encoding/json"
	"pojo"
	"os"
	"ChannelList"
	_"log"
	"strconv"
)

func ReadDirectory(dirPath string, file os.FileInfo){

	files, err := ioutil.ReadDir(dirPath)

    if err != nil {
        ChannelList.WriteLog(err.Error())
        return
    }

    for _, file := range files{

    	fi, err := os.Stat(dirPath+"/"+file.Name())

	    if err != nil {
	        ChannelList.WriteLog(err.Error())
	        break
	    }

	    mode := fi.Mode()

	    if mode.IsDir(){

	    	ReadDirectory(dirPath+"/"+file.Name(), file)

	    }else if mode.IsRegular(){

	    	ReadFile(dirPath, file)
	    }

    }
}

func ReadFile(path string, file os.FileInfo){

	defer ChannelList.Recover()

	extension := filepath.Ext(file.Name())

    if extension == ".json"{

    	data, err := ioutil.ReadFile(path+"/"+file.Name())

		if err != nil{
			ChannelList.WriteLog(err.Error())
			return
		}

		channelMap := make(map[string]interface{})

		err = json.Unmarshal(data, &channelMap)

		if err != nil{
			ChannelList.WriteLog(err.Error())
			return
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
				WriteCallback:make(chan bool, 1),
				ChannelStorageType: channelMap["channelStorageType"].(string),
				SubscriberChannel: make(chan *pojo.PacketStruct),
				Group: make(map[string][]*pojo.PacketStruct),
				SubscriberList: make(map[string]bool),
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

func LoadTCPChannelsToMemory(){

	defer ChannelList.Recover()

    files, err := ioutil.ReadDir(*ChannelList.ConfigTCPObj.ChannelConfigFiles)

    if err != nil {
        ChannelList.WriteLog(err.Error())
        return
    }

    for _, file := range files{

    	fi, err := os.Stat(*ChannelList.ConfigTCPObj.ChannelConfigFiles+"/"+file.Name())

	    if err != nil {
	        ChannelList.WriteLog(err.Error())
	        break
	    }

	    mode := fi.Mode()

	    if mode.IsDir(){

	    	ReadDirectory(*ChannelList.ConfigTCPObj.ChannelConfigFiles+"/"+file.Name(), file)

	    }else if mode.IsRegular(){

	    	ReadFile(*ChannelList.ConfigTCPObj.ChannelConfigFiles, file)
	    }

    }
}

func openDataFile(protocol string, channelObject *pojo.ChannelStruct, channelMap map[string]interface{}) *pojo.ChannelStruct{

	defer ChannelList.Recover()

	if protocol == "tcp"{

		var partitions = int(channelMap["partitions"].(float64))

		for i:=0;i<partitions;i++{

			var filePath = channelMap["path"].(string)+"/"+channelMap["channelName"].(string)+"_partition_"+strconv.Itoa(i)+".br"

			f, err := os.OpenFile(filePath,
				os.O_APPEND, os.ModeAppend)

			if err != nil {
				ChannelList.WriteLog(err.Error())
				break
			}

			channelObject.FD = append(channelObject.FD, f)

		}

		channelObject.PartitionCount = int(channelMap["partitions"].(float64))
		channelObject.Path = channelMap["path"].(string)
		
	}else if protocol == "udp"{


	}

	return channelObject
}

func LoadUDPChannelsToMemory(){

    
}