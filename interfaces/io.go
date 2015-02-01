package interfaces

type Input interface {
	GetName() string
	Begin(outputChannel chan<- Reading, endChannel <-chan interface{})
}

type Output interface {
	GetName() string
	Begin(inputChannel <-chan Reading, endChannel <-chan interface{})
}
