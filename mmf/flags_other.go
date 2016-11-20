// +build !linux

package mmf

import (
	"os"
)

const createFlags = os.O_RDWR | os.O_CREATE | os.O_TRUNC
const openFlags = os.O_RDWR | os.O_CREATE
