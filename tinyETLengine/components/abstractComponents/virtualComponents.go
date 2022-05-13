package abstractComponents

type VirtualComponents interface {
	SetId(id string)
	GetId() string
	Run(indata chan interface{},outdata chan interface{},datameta map[string]map[string]interface{})
	GetDataMeta()  map[string]map[string]interface{}
}
