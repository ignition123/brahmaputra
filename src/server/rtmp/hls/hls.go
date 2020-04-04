package hls

import (
	"server/rtmp/util"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	HLS_KEY_METHOD_AES_128 = "AES-128"
)

type Playlist struct {

	ExtM3U         string   
	Version        int      
	Sequence       int       
	Targetduration int     
	PlaylistType   int      
	Discontinuity  int        
	Key            PlaylistKey
	EndList        string     
	Inf            PlaylistInf

}

type PlaylistKey struct{

	Method string // specifies the encryption method. (4.3.2.4)
	Uri    string // key url. (4.3.2.4)
	IV     string // key iv. (4.3.2.4)

}

type PlaylistInf struct{

	Duration float64
	Title    string

}

func (this *Playlist) Init(filename string) (err error){

	defer this.handleError()

	if util.Exist(filename){

		if err = os.Remove(filename); err != nil{

			return

		}

	}

	var file *os.File

	file, err = os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0644)

	if err != nil{

		return

	}

	defer file.Close()

	ss := fmt.Sprintf("#EXTM3U\n"+
		"#EXT-X-VERSION:%d\n"+
		"#EXT-X-MEDIA-SEQUENCE:%d\n"+
		"#EXT-X-TARGETDURATION:%d\n", this.Version, this.Sequence, this.Targetduration)

	if _, err = file.WriteString(ss); err != nil{

		return

	}

	file.Close()

	return
}

func (this *Playlist) WriteInf(filename string, inf PlaylistInf) (err error){

	defer this.handleError()

	var file *os.File

	file, err = os.OpenFile(filename, os.O_WRONLY|os.O_APPEND, 0644)

	if err != nil{

		return

	}

	defer file.Close()

	ss := fmt.Sprintf("#EXTINF:%.3f,\n"+
		"%s\n", inf.Duration, inf.Title)

	if _, err = file.WriteString(ss); err != nil{

		return

	}

	file.Close()

	return
}

func (this *Playlist) UpdateInf(filename string, tmpFilename string, inf PlaylistInf) (err error){

	var oldContent []string

	var newContent string

	var tmpFile *os.File

	tmpFile, err = os.OpenFile(tmpFilename, os.O_WRONLY|os.O_CREATE, 0644)

	if err != nil{

		return

	}

	defer tmpFile.Close()

	var ls []string

	if ls, err = util.ReadFileLines(filename); err != nil{

		return

	}

	for i, v := range ls{

		if strings.Contains(v, "#EXT-X-MEDIA-SEQUENCE"){

			var index, seqNum int

			var oldStrNum, newStrNum, newSeqStr string

			if index = strings.Index(v, ":"); index == -1{

				err = errors.New("#EXT-X-MEDIA-SEQUENCE not found.")

				return

			}

			oldStrNum = v[index+1:]

			seqNum, err = strconv.Atoi(oldStrNum)

			if err != nil{

				return

			}

			seqNum++

			newStrNum = strconv.Itoa(seqNum)

			newSeqStr = strings.Replace(v, oldStrNum, newStrNum, 1)

			ls[i] = newSeqStr

		}

		if strings.Contains(v, "#EXTINF"){

			oldContent = append(ls[0:i], ls[i+2:]...)

			break

		}

	}

	for _, v := range oldContent{

		newContent += v + "\n"

	}

	ss := fmt.Sprintf("#EXTINF:%.3f,\n"+
		"%s\n", inf.Duration, inf.Title)

	newContent += ss

	if _, err = tmpFile.WriteString(newContent); err != nil{

		return

	}

	if err = tmpFile.Close(); err != nil{

		return

	}

	if err = os.Remove(filename); err != nil{

		return

	}

	if err = os.Rename(tmpFilename, filename); err != nil{

		return

	}

	return
}

func (this *Playlist) GetInfCount(filename string) (num int, err error){

	var ls []string

	if ls, err = util.ReadFileLines(filename); err != nil{

		return

	}

	num = 0

	for _, v := range ls{

		if strings.Contains(v, "#EXTINF"){

			num++

		}

	}

	return
}

func (this *Playlist) handleError(){

	if err := recover(); err != nil{

		fmt.Println(err)

	}
	
}
