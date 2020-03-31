package udp

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

	defer ChannelList.Recover()

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

		if channelMap["type"] == "channel" && channelMap["channelType"] == "udp"{

			var channelName = channelMap["channelName"].(string)

			var channelObject = &pojo.UDPChannelStruct{
				Offset:int64(0),
				WriteCallback:make(chan bool, 1),
				ChannelStorageType: channelMap["channelStorageType"].(string),
				Group: make(map[string][]*pojo.UDPPacketStruct),
				SubscriberList: make(map[string]bool),
			}

			if *ChannelList.ConfigUDPObj.Storage.File.Active && channelName != "heart_beat"{

				if channelObject.ChannelStorageType == "persistent"{
					channelObject = openDataFile("udp", channelObject, channelMap)
				}
			}

			ChannelList.UDPStorage[channelName] = channelObject

		}	

    }

}

func LoadUDPChannelsToMemory(){

	defer ChannelList.Recover()

    files, err := ioutil.ReadDir(*ChannelList.ConfigUDPObj.ChannelConfigFiles)

    if err != nil {
        ChannelList.WriteLog(err.Error())
        return
    }

    for _, file := range files{

    	fi, err := os.Stat(*ChannelList.ConfigUDPObj.ChannelConfigFiles+"/"+file.Name())

	    if err != nil {
	        ChannelList.WriteLog(err.Error())
	        break
	    }

	    mode := fi.Mode()

	    if mode.IsDir(){

	    	ReadDirectory(*ChannelList.ConfigUDPObj.ChannelConfigFiles+"/"+file.Name(), file)

	    }else if mode.IsRegular(){

	    	ReadFile(*ChannelList.ConfigUDPObj.ChannelConfigFiles, file)
	    }

    }
}

func openDataFile(protocol string, channelObject *pojo.UDPChannelStruct, channelMap map[string]interface{}) *pojo.UDPChannelStruct{

	defer ChannelList.Recover()

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

	return channelObject
}
