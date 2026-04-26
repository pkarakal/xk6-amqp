package k6common

import "fmt"

// BaseConnectionOptions holds the parameters needed to connect to an AMQP broker.
// Both amqp091 and amqp10 packages alias this type as their own ConnectionOptions.
type BaseConnectionOptions struct {
	Host     string `json:"host,omitempty"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	Port     int    `json:"port,omitempty"`
}

// ToConnectionString formats the options as an amqp:// URL.
func (o *BaseConnectionOptions) ToConnectionString() string {
	if o == nil {
		return ""
	}
	return fmt.Sprintf("amqp://%s:%s@%s:%d", o.Username, o.Password, o.Host, o.Port)
}
