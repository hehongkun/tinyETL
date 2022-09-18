package abstractComponents

import "time"

type VirtualComponents interface {
	SetId(id string)
	GetId() string
	Run(indata *chan interface{},outdata *chan interface{},datameta map[string]map[string]interface{})
	GetDataMeta()  map[string]map[string]interface{}
	SetDataMeta(dataMeta map[string]map[string]interface{})
	SetStartTime()
	SetEndTime()
	GetStartTime() time.Time
	GetEndTime() time.Time
	GetName() string
	SetName(name string)
	SetStatus(status int)
	GetStatus() int
}
