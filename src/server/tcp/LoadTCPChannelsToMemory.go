package tcp

import(
	"ChannelList"
	"os"
	"io/ioutil"
	"path/filepath"
	"pojo"
	"encoding/json"
	"strconv"
	_"log"
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

		if channelMap["type"] == "channel"{

			channelObject := &pojo.ChannelStruct{
				ChannelStorageType: channelMap["channelStorageType"].(string),
			}

			// setting packet objects of the channel

			channelName := channelMap["channelName"].(string)

			if *ChannelList.ConfigTCPObj.Storage.File.Active && channelName != "heart_beat"{

				if channelObject.ChannelStorageType == "inmemory"{

					ChannelList.CreateSubscriberChannels(channelName, channelObject)

				}else if channelObject.ChannelStorageType == "persistent"{

					channelObject = openDataFile(channelObject, channelMap)

					ChannelList.CreateSubscriberChannels(channelName, channelObject)

				}else{

					ChannelList.WriteLog("Invalid channel type")

        			return
				}

			}

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

// opening file descriptor for the log files

func openDataFile(channelObject *pojo.ChannelStruct, channelMap map[string]interface{}) *pojo.ChannelStruct{

	defer ChannelList.Recover()

	partitions := int(channelMap["partitions"].(float64))

	for i:=0;i<partitions;i++{

		filePath := channelMap["path"].(string)+"/"+channelMap["channelName"].(string)+"_partition_"+strconv.Itoa(i)+".br"

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