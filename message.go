package nsqForSure

type keyedMessage struct {
	Key       string
	KeyLength int64
	Body      []byte
}
