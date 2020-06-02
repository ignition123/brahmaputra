package pojo

type ServerStruct struct{
	TCP TCPStruct
	ClusterName *string
	HostName *string
}

type TCPStruct struct{
	Host *string
	Port *string 
	MaxSockets *int64
	SocketTimeout *int
	BufferRead *int
}

type Config struct{
	Worker *int
	Server ServerStruct
	Dbpath *string
	Storage StorageStruct
	ChildWrite ChildWriteStruct
	ClusterWrite *bool
	ChannelConfigFiles *string 
	LogWrite bool
}

type StorageStruct struct{
	File FileStruct
}

type FileStruct struct{
	Active *bool
}

type ChildWriteStruct struct{
	WriteConcern *int
}
