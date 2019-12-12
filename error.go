package dumpling

import (
	"errors"
)

type errWithStack struct {
	raw error
}

func (e errWithStack) Unwrap() error {
	return e.raw
}

func (e errWithStack) Error() string {
	return ""
}

var stackErr errWithStack

func withStack(err error) error {
	if errors.Is(err, stackErr) {
		return err
	}

	return errWithStack{raw: err}
}
