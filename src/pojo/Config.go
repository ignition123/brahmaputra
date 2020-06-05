package pojo

// config serve rstruct

type ServerStruct struct{
	TCP TCPStruct
	ClusterName *string
	HostName *string
}

// config tcp struct

type TCPStruct struct{
	Host *string
	Port *string 
	MaxSockets *int64
	SocketTimeout *int
	BufferRead *int
}

// config struct

type Config struct{
	Worker *int
	Server ServerStruct
	Storage StorageStruct
	ChildWrite ChildWriteStruct
	ClusterWrite *bool
	ChannelConfigFiles *string 
	LogWrite bool
}

// config storage struct

type StorageStruct struct{
	File FileStruct
}

// config file struct

type FileStruct struct{
	Active *bool
}

// config write concern struct

type ChildWriteStruct struct{
	WriteConcern *int
}
