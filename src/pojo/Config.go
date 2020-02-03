package pojo

type AuthStruct struct{
	Username *string
	Password *string
}

type ServerStruct struct{
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