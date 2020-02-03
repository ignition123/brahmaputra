package pojo

type SetFunc struct{
	Key *string `json:"key"`
	Val *string `json:"val"`
}

type GetFunc struct{
	Key *string `json:"key"`
}

type DelFunc struct{
	Key *string `json:"key"`
}

type AppendFunc struct{
	Key *string `json:"key"`
	Val *string `json:"val"`
}

type SetGrpFunc struct{
	Keys *[]string `json:"keys"`
	Vals *[]string `json:"vals"`
}

type GetGrpFunc struct{
	Keys *[]string `json:"keys"`
}

type DelGrpFunc struct{
	Keys *[]string `json:"keys"`
}

type ChQuery struct{
	DB *int `json:"db"`
	Set *SetFunc
	Get *GetFunc
	Del *DelFunc
	Append *AppendFunc
	SetGrp *SetGrpFunc
	GetGrp *GetGrpFunc
	DelGrp *DelGrpFunc
}

type QueryParser struct{
	Ch *ChQuery
}