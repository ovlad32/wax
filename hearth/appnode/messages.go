package appnode



type ParishRequestType struct {
	SlaveNodeName string
	CommandSubjet string
}

type ParishResponseType struct {
	Registered bool
	Err error
}