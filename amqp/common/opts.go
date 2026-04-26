package k6common

import (
	"encoding/json"
	"fmt"
)

// ConvertOpts coerces opts to *T.
// It accepts either a *T directly (Go callers) or a map[string]any (sobek JS
// callers, which reflect JS objects as maps before passing them to Go).
func ConvertOpts[T any](method string, opts any) (*T, error) {
	if o, ok := opts.(*T); ok {
		return o, nil
	}
	m, ok := opts.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s: unexpected options type %T", method, opts)
	}
	data, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to marshal options: %w", method, err)
	}
	target := new(T)
	if err := json.Unmarshal(data, target); err != nil {
		return nil, fmt.Errorf("%s: failed to unmarshal options: %w", method, err)
	}
	return target, nil
}
