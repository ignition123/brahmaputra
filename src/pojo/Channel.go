package pojo

import(
	"os"
)

// channel config struct

type ChannelStruct struct{
	FD []*os.File 
	Path string
	ChannelStorageType string
	PartitionCount int
}