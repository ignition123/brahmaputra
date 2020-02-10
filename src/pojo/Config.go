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
}

type StorageStruct struct{
	File FileStruct
	Mongodb MongodbStruct
	MySQL MySQStruct
	Hbase HbaseStruct
	CouchDB CouchDBStruct
	Cassandra CassandraStruct
	Postgres PostgresStruct
}

type FileStruct struct{
	Active *bool
}

type MongodbStruct struct{
	Active *bool
	Url *string
}

type MySQStruct struct{
	Active *bool
}

type HbaseStruct struct{
	Active *bool
}

type CouchDBStruct struct{
	Active *bool
}

type CassandraStruct struct{
	Active *bool
}

type PostgresStruct struct{
	Active *bool
}

type ChildWriteStruct struct{
	WriteConcern *int
}
