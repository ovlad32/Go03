package csm

type infoType struct {
}
type arrayInfoType []struct{}
type MessageType struct {
	arrayInfoType
	infoType
}
type ChannelType chan MessageType

func NewMessage() MessageType {
	return new(infoType)
}

func NewMessageSize(size int) MessageType {
	return make(arrayInfoType, 0, size)
}

func NewChannel() ChannelType {
	return make(chan ChannelType)
}

func NewChannelSize(size int) ChannelType {
	return make(chan ChannelType, size)
}

func (m *MessageType) Put(info struct{}) *MessageType {
	*m.infoType = info
	return m
}

func (m *MessageType) PutN(index int, info struct{}) *MessageType {
	(*m)[index] = info
	return m
}
func (m MessageType) Get() struct{} {
	return m.infoType
}

func (m MessageType) GetN(index int) struct{} {
	return m.arrayInfoType[index]
}
