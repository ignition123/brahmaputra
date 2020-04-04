package rtmp

import(
	"io/ioutil"
	"path/filepath"
	"encoding/json"
	"pojo"
	"os"
	"ChannelList"
	_"log"
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

		if channelMap["type"] == "channel" && channelMap["channelType"] == "rtmp"{

			var channelName = channelMap["channelName"].(string)

			var hlsMap = channelMap["hls"].(map[string]interface{})

			var onPublishMap = channelMap["onPublish"].(map[string]interface{})

			var onPlayMap = channelMap["onPlay"].(map[string]interface{})

			var onEndMap = channelMap["onEnd"].(map[string]interface{})

			var hls = &pojo.HlsStruct{}

			if hlsMap["hls_path"] != nil && hlsMap["hls_path"].(string) != ""{
				hls.Hls_fragment = int(hlsMap["hls_fragment"].(float64))
				hls.Hls_window = int(hlsMap["hls_window"].(float64))
				hls.Hls_path = hlsMap["hls_path"].(string)
			}

			var onPublish = &pojo.OnPublishStruct{}

			if onPublishMap["authUrl"] != nil && onPublishMap["authUrl"].(string) != ""{

				onPublish.AuthUrl = onPublishMap["authUrl"].(string)

			}

			if onPublishMap["hookCall"] != nil && onPublishMap["hookCall"].(string) != ""{

				onPublish.HookCall = onPublishMap["hookCall"].(string)

			}

			if onPublishMap["exec"] != nil && onPublishMap["exec"].(string) != ""{

				onPublish.Exec = onPublishMap["exec"].(string)

			}

			var onOnPlay = &pojo.OnPlayStruct{}

			if onPlayMap["authUrl"] != nil && onPlayMap["authUrl"].(string) != ""{

				onOnPlay.AuthUrl = onPlayMap["authUrl"].(string)

			}

			if onPlayMap["hookCall"] != nil && onPlayMap["hookCall"].(string) != ""{

				onOnPlay.HookCall = onPlayMap["hookCall"].(string)

			}

			if onPlayMap["exec"] != nil && onPlayMap["exec"].(string) != ""{

				onOnPlay.Exec = onPlayMap["exec"].(string)

			}

			var onEnd = &pojo.OnEndStruct{}

			if onEndMap["hookCall"] != nil && onEndMap["hookCall"].(string) != ""{

				onEnd.HookCall = onEndMap["hookCall"].(string)

			}

			if onEndMap["exec"] != nil && onEndMap["exec"].(string) != ""{

				onEnd.Exec = onEndMap["exec"].(string)

			}

			var channelObject = &pojo.RTMPChannelStruct{
				ChannelName:channelName,
				Hls: *hls,
				OnEnd: *onEnd,
				OnPlay: *onOnPlay,
				OnPublish: *onPublish,
			}

			ChannelList.RTMPStorage[channelName] = channelObject

		}	

    }

}

func LoadRTMPChannelsToMemory(){

	defer ChannelList.Recover()

    files, err := ioutil.ReadDir(*ChannelList.ConfigRTMPObj.ChannelConfigFiles)

    if err != nil {
        ChannelList.WriteLog(err.Error())
        return
    }

    for _, file := range files{

    	fi, err := os.Stat(*ChannelList.ConfigRTMPObj.ChannelConfigFiles+"/"+file.Name())

	    if err != nil {
	        ChannelList.WriteLog(err.Error())
	        break
	    }

	    mode := fi.Mode()

	    if mode.IsDir(){

	    	ReadDirectory(*ChannelList.ConfigRTMPObj.ChannelConfigFiles+"/"+file.Name(), file)

	    }else if mode.IsRegular(){

	    	ReadFile(*ChannelList.ConfigRTMPObj.ChannelConfigFiles, file)
	    }

    }
}
