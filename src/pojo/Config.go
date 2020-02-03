package pojo

type AuthStruct struct{
	Username *string
	Password *string
}

type ServerStruct struct{
	TCP TCPStruct
	UDP UDPStruct
}

type TCPStruct struct{
	Host *string
	Port *string 
	MaxSockets *int
	SocketTimeout *int
}

type UDPStruct struct{
	Host *string
	Port *string 
	MaxSockets *int
	SocketTimeout *int
}

type Config struct{
	Worker *int
	Server ServerStruct
	Dbpath *string
	Auth AuthStruct
}