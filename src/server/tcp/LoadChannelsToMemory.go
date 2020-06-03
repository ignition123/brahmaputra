package tcp

/*

	This file contains all the methods related to create packetObject 
	and opening config file of each channel and loading data into 
	inmemory of the channel details
	
*/

// importing modules

import(
	"io/ioutil"
	"path/filepath"
	"encoding/json"
	"pojo"
	"os"
	"ChannelList"
	"strconv"
)

// memthod to read the directory defined in the config file

func ReadDirectory(dirPath string, file os.FileInfo){

	defer ChannelList.Recover()

	// reading the directory

	files, err := ioutil.ReadDir(dirPath)

    if err != nil {
        ChannelList.WriteLog(err.Error())
        return
    }

    // iterating each files

    for _, file := range files{

    	// getting the file stats

    	fi, err := os.Stat(dirPath+"/"+file.Name())

	    if err != nil {
	        ChannelList.WriteLog(err.Error())
	        break
	    }

	    // checking if the path is a directory of file

	    mode := fi.Mode()

	    if mode.IsDir(){

	    	// reading directory

	    	ReadDirectory(dirPath+"/"+file.Name(), file)

	    }else if mode.IsRegular(){

	    	// reading files

	    	ReadFile(dirPath, file)
	    }

    }
}

// reading config json files

func ReadFile(path string, file os.FileInfo){

	defer ChannelList.Recover()

	// reading the file extension

	extension := filepath.Ext(file.Name())

	// if extension is json 

    if extension == ".json"{

    	// reading the json content

    	data, err := ioutil.ReadFile(path+"/"+file.Name())

		if err != nil{
			ChannelList.WriteLog(err.Error())
			return
		}

		// loading the json to hashmap

		channelMap := make(map[string]interface{})

		err = json.Unmarshal(data, &channelMap)

		if err != nil{
			ChannelList.WriteLog(err.Error())
			return
		}

		// if the channel type is tcp, udp was also in the option is removed for now for data loss

		if channelMap["type"] == "channel" && channelMap["channelType"] == "tcp"{

			// getting number of workers, used for inmemory distribution

			worker := int(channelMap["worker"].(float64))

			// creating bucketData channel

			bucketData  := make([]chan *pojo.PacketStruct, worker)

			for i := range bucketData {
			   bucketData[i] = make(chan *pojo.PacketStruct, *ChannelList.ConfigTCPObj.Server.TCP.BufferRead)
			}

			// setting packet objects of the channel

			channelName := channelMap["channelName"].(string)

			channelObject := &pojo.ChannelStruct{
				Offset:int64(0),
				Worker: worker,
				BucketData: bucketData,
				WriteCallback:make(chan bool, 1),
				SyncChan: make(chan bool, 1),
				ChannelStorageType: channelMap["channelStorageType"].(string),
				Group: make(map[string][]*pojo.PacketStruct),
				SubscriberList: make(map[string]bool),
			}

			if *ChannelList.ConfigTCPObj.Storage.File.Active && channelName != "heart_beat"{

				if channelObject.ChannelStorageType == "persistent"{
					channelObject = openDataFile("tcp", channelObject, channelMap)
				}
			}

			ChannelList.TCPStorage[channelName] = channelObject

			ChannelList.TCPSocketDetails[channelName] = make(map[string] *pojo.PacketStruct)
		}	

    }

}

// method to read the directory and open file config

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

// opening file descriptor for the log files

func openDataFile(protocol string, channelObject *pojo.ChannelStruct, channelMap map[string]interface{}) *pojo.ChannelStruct{

	defer ChannelList.Recover()

	var partitions = int(channelMap["partitions"].(float64))

	for i:=0;i<partitions;i++{

		var filePath = channelMap["path"].(string)+"/"+channelMap["channelName"].(string)+"_partition_"+strconv.Itoa(i)+".br"

		f, err := os.OpenFile(filePath,
			os.O_APPEND|os.O_WRONLY, os.ModeAppend)

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
