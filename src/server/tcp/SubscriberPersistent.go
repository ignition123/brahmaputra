package tcp

import(
	"ChannelList"
	"objects"
	"os"
	"io/ioutil"
	"encoding/binary"
)

func checkCreateDirectory(clientObj *objects.ClientObject, packetObject *objects.PacketStruct, checkDirectoryChan chan bool){

	defer ChannelList.Recover()

	// declaring consumerName

	consumerName := packetObject.ChannelName + packetObject.SubscriberName

	// declaring directory path

	directoryPath := objects.SubscriberObj[packetObject.ChannelName].Channel.Path+"/"+consumerName

	if _, err := os.Stat(directoryPath); err == nil{

		checkDirectoryChan <- true

	}else if os.IsNotExist(err){ // if not exists then create directory

		errDir := os.MkdirAll(directoryPath, 0755)

		if errDir != nil { //  if error then throw error
			
			ChannelList.ThroughClientError(clientObj.Conn, err.Error())

			// deleting the subscriber from the list, locking the channel list array using mutex

			objects.SubscriberObj[packetObject.ChannelName].UnRegister <- clientObj

			checkDirectoryChan <- false

			return

		}

		// else subscriber directory created return true

		go ChannelList.WriteLog("Subscriber directory created successfully...")

		checkDirectoryChan <- true

	}else{

		checkDirectoryChan <- false

	}

}

func createSubscriberOffsetFile(clientObj *objects.ClientObject, packetObject *objects.PacketStruct, start_from string, partitionOffsetSubscriber chan int64){

	defer ChannelList.Recover()

	// declaring consumer name

	consumerName := packetObject.ChannelName + packetObject.SubscriberName

	// declaring directory path

	directoryPath := objects.SubscriberObj[packetObject.ChannelName].Channel.Path+"/"+consumerName

	// declaring the consumer offset path

	consumerOffsetPath := directoryPath+"\\"+packetObject.SubscriberName+"_offset_.index"

	// checking the os stat of the path

	if _, err := os.Stat(consumerOffsetPath); err == nil{

		// opening the file and setting file descriptor

		fDes, err := os.OpenFile(consumerOffsetPath,
			os.O_WRONLY, os.ModeAppend)

		// if error 

		if err != nil {

			ChannelList.ThroughClientError(clientObj.Conn, err.Error())

			objects.SubscriberObj[packetObject.ChannelName].UnRegister <- clientObj

			partitionOffsetSubscriber <- int64(-2)

			return
		}

		// adding file descriptor object to packetObject

		packetObject.SubscriberFD = fDes

		// if the start from flag is beginning then the offset will bet set to 0

		if start_from == "BEGINNING"{

			partitionOffsetSubscriber <- int64(0)

		}else if start_from == "NOPULL"{ // if no pull the offset will be equals to filelength

			partitionOffsetSubscriber <- int64(-1)

		}else if start_from == "LASTRECEIVED"{ // if last received then it will read the offset file and set the offset

			dat, err := ioutil.ReadFile(consumerOffsetPath)

			if err != nil{ // if error not equals to null then error

				ChannelList.ThroughClientError(clientObj.Conn, err.Error())

				objects.SubscriberObj[packetObject.ChannelName].UnRegister <- clientObj

				partitionOffsetSubscriber <- int64(-2)

				return

			} 

			// checking the length of the data that is being read from the file, if error then set to 0 else offet that is in the file

			if len(dat) == 0{

				partitionOffsetSubscriber <- int64(0)

			}else{

				partitionOffsetSubscriber <- int64(binary.BigEndian.Uint64(dat))

			}

		}else{

			partitionOffsetSubscriber <- int64(0)

		}

	}else if os.IsNotExist(err){ // if error in file existence

		// creating file

		fDes, err := os.Create(consumerOffsetPath)

		if err != nil{

			ChannelList.ThroughClientError(clientObj.Conn, err.Error())

			objects.SubscriberObj[packetObject.ChannelName].UnRegister <- clientObj

			partitionOffsetSubscriber <- int64(-2)

			return

		}

		// then setting the file descriptor

		packetObject.SubscriberFD = fDes

		if start_from == "BEGINNING"{

			partitionOffsetSubscriber <- int64(0)

		}else if start_from == "NOPULL"{

			partitionOffsetSubscriber <- int64(-1)

		}

	}else{

		// if start_from is BEGINNING, then offset will be 0

		if start_from == "BEGINNING"{

			partitionOffsetSubscriber <- int64(0)

		}else if start_from == "NOPULL"{ // if NOPULL the -1, means offset will be total file length

			partitionOffsetSubscriber <- int64(-1)

		}else if start_from == "LASTRECEIVED"{ // if LASTRECEIVED then it will read file and get the last offset received by the file

			dat, err := ioutil.ReadFile(consumerOffsetPath)

			if err != nil{

				ChannelList.ThroughClientError(clientObj.Conn, err.Error())

				objects.SubscriberObj[packetObject.ChannelName].UnRegister <- clientObj

				partitionOffsetSubscriber <- int64(-2)

				return

			}

			// length of the data == 0 then offset will be 0

			if len(dat) == 0{

				partitionOffsetSubscriber <- int64(0)

			}else{ // else the data that is in the file

				partitionOffsetSubscriber <- int64(binary.BigEndian.Uint64(dat))

			}

		}else{

			// sending offset as 0

			partitionOffsetSubscriber <- int64(0)

		}

	}

}