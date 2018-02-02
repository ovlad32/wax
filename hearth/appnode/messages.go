package appnode



type ParishRequestType struct {
	SlaveNodeName NodeNameType
	CommandSubject string
}

type ParishResponseType struct {
	ReConnect bool
	Err error
}