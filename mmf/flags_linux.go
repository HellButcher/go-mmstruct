package mmf

import (
	"os"

	syscall "golang.org/x/sys/unix"
)

const CREATE_FLAGS = os.O_RDWR | os.O_CREATE | syscall.O_NOATIME | os.O_TRUNC
const OPEN_FLAGS = os.O_RDWR | os.O_CREATE | syscall.O_NOATIME
