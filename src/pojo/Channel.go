package pojo

import(
	"os"
	"sync"
)

// channel config struct

type ChannelStruct struct{
	FD []*os.File 
	Path string
	ChannelStorageType string
	PartitionCount int
	ChannelLock sync.RWMutex
	SubscriberFileChannelLock sync.RWMutex
}