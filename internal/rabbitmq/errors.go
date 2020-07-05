package rabbitmq

type (
	NotConnectedError  struct{}
	AlreadyClosedError struct{}
	ShutdownError      struct{}
)

func (e *NotConnectedError) Error() string {
	return "not connected to a server"
}

func (e *AlreadyClosedError) Error() string {
	return "already closed: not connected to the server"
}

func (e *ShutdownError) Error() string {
	return "session is shutting down"
}
