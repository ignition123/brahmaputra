package pojo

type AuthStruct struct{
	Username *string
	Password *string
}

type ServerStruct struct{
	TCP TCPStruct
	UDP UDPStruct
	ClusterName *string
	HostName *string
}

type TCPStruct struct{
	Host *string
	Port *string 
	MaxSockets *int
	SocketTimeout *int
	BufferRead *int
	TotalConnection *int64
}

type UDPStruct struct{
	Host *string
	Port *string 
	MaxSockets *int
	SocketTimeout *int
	BufferRead *int
	TotalConnection *int64
}

type Config struct{
	Worker *int
	Server ServerStruct
	Dbpath *string
	Auth AuthStruct
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
