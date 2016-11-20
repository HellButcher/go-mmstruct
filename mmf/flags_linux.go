package mmf

import (
	"os"

	syscall "golang.org/x/sys/unix"
)

const createFlags = os.O_RDWR | os.O_CREATE | syscall.O_NOATIME | os.O_TRUNC
const openFlags = os.O_RDWR | os.O_CREATE | syscall.O_NOATIME
