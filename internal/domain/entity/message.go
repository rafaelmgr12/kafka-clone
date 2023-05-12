package entity

type Message struct {
	Headers map[string]string `json:"headers,omitempty"`
	Body    string            `json:"body"`
}
