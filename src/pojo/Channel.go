package pojo

type ChannelStruct struct{
	Path string
	WriteInterval int32
	BucketData chan map[string]interface{}
}