// Package amqp only exists to register the AMQP extension modules.
package amqp

import (
	amqp091module "github.com/pkarakal/xk6-amqp/amqp"
	amqp10module "github.com/pkarakal/xk6-amqp/amqp/amqp10"
	"go.k6.io/k6/js/modules"
)

func init() {
	// k6/x/amqp and k6/x/amqp091 both resolve to the AMQP 0.9.1 implementation.
	// k6/x/amqp is kept for backward compatibility with existing scripts.
	modules.Register("k6/x/amqp", new(amqp091module.RootModule))
	modules.Register("k6/x/amqp091", new(amqp091module.RootModule))

	// k6/x/amqp10 resolves to the AMQP 1.0 implementation.
	modules.Register("k6/x/amqp10", new(amqp10module.RootModule))
}
