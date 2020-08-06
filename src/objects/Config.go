package objects

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
	SocketReadTimeout *int
	SocketWriteTimeout *int
	BufferRead *int
	Timeout *int
	Linger *int
	KeepAlive *bool
	NoDelay *bool
	ReadBuffer *int
	WriteBuffer *int
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
