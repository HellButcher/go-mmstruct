// +build !linux

package mmf

import (
	"os"
)

const CREATE_FLAGS = os.O_RDWR | os.O_CREATE | os.O_TRUNC
const OPEN_FLAGS = os.O_RDWR | os.O_CREATE
