package amqp10

import k6common "github.com/pkarakal/xk6-amqp/amqp/common"

// convertOpts is a package-local alias for the shared ConvertOpts helper.
func convertOpts[T any](method string, opts any) (*T, error) {
	return k6common.ConvertOpts[T](method, opts)
}
