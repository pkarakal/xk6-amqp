// Package amqp10 implements the AMQP 1.0 k6 extension module.
package amqp10

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/grafana/sobek"
	k6common "github.com/grafana/xk6-amqp/amqp/common"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
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
		*Client
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
	return &ModuleInstance{vu: vu, Client: NewClient(vu, nil, nil)}
}

// Exports implements the modules.Instance interface and returns
// the exports of the JS module.
func (mi *ModuleInstance) Exports() modules.Exports {
	return modules.Exports{Named: map[string]any{
		"Client": mi.newClient,
	}}
}

// newClient is the JS constructor for the AMQP 1.0 Client.
//
// It accepts a single options object:
//
//	new Client({
//	  connectionOptions: { host, port, username, password },
//	  options: { /* AmqpConnOptions fields */ },
//	})
//
// The returned client is initially disconnected; the connection is established
// lazily on the first publish/listen/management call.
// jsClient is a thin wrapper around *Client exposed to sobek.
// It promotes all methods from *Client and overrides Listen with a sobek-native
// signature so the JS callback can be extracted correctly.
type jsClient struct {
	*Client
	rt *sobek.Runtime
}

// Listen shadows Client.Listen with a sobek-native signature so that the JS
// function argument can be extracted before forwarding to the real impl.
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

func (mi *ModuleInstance) newClient(call sobek.ConstructorCall) *sobek.Object {
	rt := mi.vu.Runtime()

	if len(call.Arguments) != 1 {
		common.Throw(rt, errors.New("Client constructor requires exactly one argument"))
	}

	opts, err := readOptions(call.Arguments[0].Export())
	if err != nil {
		common.Throw(rt, err)
	}

	client := NewClient(mi.vu, opts.Options, opts.ConnectionOptions)
	return rt.ToValue(&jsClient{Client: client, rt: rt}).ToObject(rt)
}

// readListenOptions deserializes ListenOptions from a sobek value, extracting
// the listener callback separately (JSON marshaling cannot handle functions).
func readListenOptions(rt *sobek.Runtime, val sobek.Value) (*ListenOptions, error) {
	exported := val.Export()
	m, ok := exported.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("listen options must be an object, got %T", exported)
	}

	// Extract the listener before JSON round-trip (functions cannot be marshaled).
	_, hasListener := m["listener"]
	delete(m, "listener")

	data, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize listen options: %w", err)
	}
	opts := &ListenOptions{}
	if err := json.Unmarshal(data, opts); err != nil {
		return nil, fmt.Errorf("invalid listen options: %w", err)
	}

	if !hasListener {
		return nil, errors.New("listen options must include a listener function")
	}

	obj := val.ToObject(rt)
	listenerVal := obj.Get("listener")
	if listenerVal == nil || sobek.IsUndefined(listenerVal) || sobek.IsNull(listenerVal) {
		return nil, errors.New("listen options must include a listener function")
	}

	callable, ok := sobek.AssertFunction(listenerVal)
	if !ok {
		return nil, errors.New("listener must be a function")
	}

	opts.Listener = func(msg string) error {
		_, err := callable(sobek.Undefined(), rt.ToValue(msg))
		return err
	}

	return opts, nil
}

type clientOptions struct {
	Options           *rmq.AmqpConnOptions `json:"options,omitempty"`
	ConnectionOptions *ConnectionOptions   `json:"connectionOptions,omitempty"`
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
var _ k6common.ConnectionOptions = (*ConnectionOptions)(nil)
