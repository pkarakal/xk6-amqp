// Package amqp implements the AMQP 0.9.1 k6 extension module.
package amqp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/grafana/sobek"
	"github.com/pkarakal/xk6-amqp/amqp/amqp091"
	k6common "github.com/pkarakal/xk6-amqp/amqp/common"
	rmqamqp "github.com/rabbitmq/amqp091-go"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
)

type (
	// RootModule is the global module instance that will create ModuleInstance
	// instances for each VU.
	RootModule struct{}

	// ModuleInstance represents an instance of the JS module for a single VU.
	ModuleInstance struct {
		vu modules.VU
		*amqp091.Client
	}
)

// Ensure the interfaces are implemented correctly.
var (
	_ modules.Instance = &ModuleInstance{}
	_ modules.Module   = &RootModule{}
)

// New returns a pointer to a new RootModule instance.
func New() *RootModule {
	return &RootModule{}
}

// NewModuleInstance implements the modules.Module interface and returns
// a new instance for each VU.
func (*RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	return &ModuleInstance{vu: vu, Client: amqp091.NewClient(vu, nil, nil)}
}

// Exports implements the modules.Instance interface and returns
// the exports of the JS module.
func (mi *ModuleInstance) Exports() modules.Exports {
	return modules.Exports{Named: map[string]any{
		"Client": mi.newClient,
	}}
}

// jsClient is a thin wrapper around *amqp091.Client exposed to sobek.
// It promotes all methods from *amqp091.Client and overrides Listen with a
// sobek-native signature so the JS callback can be extracted correctly.
// (Sobek host objects are read-only; overriding a reflected method via Set is
// not possible, so the shadow must be present at reflect time.)
type jsClient struct {
	*amqp091.Client
	rt *sobek.Runtime
}

// Listen shadows amqp091.Client.Listen with a sobek-native signature so that
// the JS function argument can be extracted before forwarding to the real impl.
func (j *jsClient) Listen(call sobek.FunctionCall) sobek.Value {
	if len(call.Arguments) != 1 {
		common.Throw(j.rt, errors.New("listen requires exactly one argument"))
	}
	opts, err := readListenOptions(j.rt, call.Arguments[0])
	if err != nil {
		common.Throw(j.rt, err)
	}
	if err := j.Client.Listen(opts); err != nil {
		common.Throw(j.rt, err)
	}
	return sobek.Undefined()
}

// newClient is the JS constructor for the AMQP 0.9.1 Client.
//
// It accepts a single options object:
//
//	new Client({
//	  connectionOptions: { host, port, username, password },
//	  options: { /* amqp091-go Config fields */ },
//	})
//
// The returned client is initially disconnected; the connection is established
// lazily on the first publish/listen/management call.
func (mi *ModuleInstance) newClient(call sobek.ConstructorCall) *sobek.Object {
	rt := mi.vu.Runtime()

	if len(call.Arguments) != 1 {
		common.Throw(rt, errors.New("Client constructor requires exactly one argument"))
	}

	opts, err := readOptions(call.Arguments[0].Export())
	if err != nil {
		common.Throw(rt, err)
	}

	client := amqp091.NewClient(mi.vu, opts.Options, opts.ConnectionOptions)
	return rt.ToValue(&jsClient{Client: client, rt: rt}).ToObject(rt)
}

// readListenOptions deserializes a ListenOptions from a sobek value using the
// shared ExtractListener helper to pull out the JS callback.
func readListenOptions(rt *sobek.Runtime, val sobek.Value) (*amqp091.ListenOptions, error) {
	cleanedMap, listener, err := k6common.ExtractListener(rt, val)
	if err != nil {
		return nil, err
	}

	data, err := json.Marshal(cleanedMap)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize listen options: %w", err)
	}
	opts := &amqp091.ListenOptions{}
	if err := json.Unmarshal(data, opts); err != nil {
		return nil, fmt.Errorf("invalid listen options: %w", err)
	}

	opts.Listener = listener
	return opts, nil
}

type clientOptions struct {
	Options           *rmqamqp.Config            `json:"options,omitempty"`
	ConnectionOptions *amqp091.ConnectionOptions `json:"connectionOptions,omitempty"`
}

func readOptions(obj any) (*clientOptions, error) {
	val, ok := obj.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid options type: %T; expected object", obj)
	}
	opts, err := newOptionsFromObject(val)
	if err != nil {
		return nil, fmt.Errorf("invalid options: %w", err)
	}
	return opts, nil
}

// newOptionsFromObject validates and instantiates a clientOptions struct from
// its map representation as exported from sobek.Runtime.
func newOptionsFromObject(obj map[string]any) (*clientOptions, error) {
	opts := &clientOptions{}
	jsonStr, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("unable to serialize options to JSON: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(jsonStr))
	decoder.DisallowUnknownFields()

	if err = decoder.Decode(opts); err != nil {
		return nil, err
	}

	return opts, nil
}

// Ensure ConnectionOptions satisfies the common interface.
var _ k6common.ConnectionOptions = (*amqp091.ConnectionOptions)(nil)
