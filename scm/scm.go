package csm

type infoType interface {
}
type arrayInfoType []interface{}

type MessageType struct {
	arrayInfoType
	infoType
}
type ChannelType chan *MessageType

func NewMessage() *MessageType {
	return &MessageType{}
}

func NewMessageSize(size int) *MessageType {
	return &MessageType{
		arrayInfoType: make(arrayInfoType, size, size),
	}

}

func NewChannel() ChannelType {
	return make(ChannelType)
}

func NewChannelSize(size int) ChannelType {
	return make(ChannelType, size)
}

func (m *MessageType) Put(info interface{}) *MessageType {

	(*m).infoType = info
	return m
}

func (m *MessageType) PutN(index int, info interface{}) *MessageType {
	(*m).arrayInfoType[index] = info
	return m
}
func (m MessageType) Get() interface{} {
	return m.infoType
}

func (m MessageType) GetN(index int) interface{} {

	return m.arrayInfoType[index]
}
