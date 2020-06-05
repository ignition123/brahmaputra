package pojo

// mongodb struct

type MongoFields struct {
	Id int64 `json:"_id"`
	LUT int64 `json:"LUT"`
	Data []byte `json:"data"`
}