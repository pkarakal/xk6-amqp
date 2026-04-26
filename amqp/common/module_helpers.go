package k6common

import (
	"errors"
	"fmt"

	"github.com/grafana/sobek"
)

// ExtractListener pulls the "listener" JS function out of a sobek options
// value. It returns:
//   - cleanedMap: the options map with "listener" removed, safe to JSON-marshal
//     into a protocol-specific ListenOptions struct.
//   - listener: a Go function wrapping the JS callable, using the Message
//     envelope signature.
//   - err: non-nil if the value is not an object, the listener key is missing,
//     or the listener is not a function.
func ExtractListener(rt *sobek.Runtime, val sobek.Value) (map[string]any, ListenerType, error) {
	exported := val.Export()
	m, ok := exported.(map[string]any)
	if !ok {
		return nil, nil, fmt.Errorf("listen options must be an object, got %T", exported)
	}

	_, hasListener := m["listener"]
	delete(m, "listener")

	if !hasListener {
		return nil, nil, errors.New("listen options must include a listener function")
	}

	obj := val.ToObject(rt)
	listenerVal := obj.Get("listener")
	if listenerVal == nil || sobek.IsUndefined(listenerVal) || sobek.IsNull(listenerVal) {
		return nil, nil, errors.New("listen options must include a listener function")
	}

	callable, ok := sobek.AssertFunction(listenerVal)
	if !ok {
		return nil, nil, errors.New("listener must be a function")
	}

	listener := func(msg *Message) error {
		_, err := callable(sobek.Undefined(), rt.ToValue(msg))
		return err
	}

	return m, listener, nil
}
