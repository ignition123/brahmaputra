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
	BufferRead *int
	ClusterName *string
	HostName *string
	TotalConnection *int64
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
	Storage StorageStruct
	ChildWrite ChildWriteStruct
	ClusterWrite *bool
	ChannelConfigFiles *string 
}

type StorageStruct struct{
	File FileStruct
	Mongodb MongodbStruct
	MySQL MySQStruct
	Cassandra CassandraStruct
}

type FileStruct struct{
	Active *bool
}

type MongodbStruct struct{
	Active *bool
	Url *string
	MinPoolSize *uint64
	MaxPoolSize *uint64
}

type MySQStruct struct{
	Active *bool
}

type CassandraStruct struct{
	Active *bool
}

type ChildWriteStruct struct{
	WriteConcern *int
}
