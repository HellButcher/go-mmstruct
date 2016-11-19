// +build !windows

package mmf

import (
	"os"
	"unsafe"

	syscall "golang.org/x/sys/unix"
)

type MappedFile struct {
	data []byte
	off  int
	file *os.File
}

func (self *MappedFile) mmap(size int) error {
	var err error
	self.data, err = syscall.Mmap(int(self.file.Fd()), 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return os.NewSyscallError("Mmap", err)
	}
	return nil
}

func (self *MappedFile) munmap() error {
	if data := self.data; data != nil {
		self.data = nil
		if err := syscall.Munmap(data); err != nil {
			return os.NewSyscallError("Munmap", err)
		}
	}
	return nil
}

func (self *MappedFile) sync(async bool) error {
	var flags uintptr
	if async {
		flags = syscall.MS_ASYNC
	} else {
		flags = syscall.MS_SYNC
	}
	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC, uintptr(unsafe.Pointer(&self.data[0])), uintptr(len(self.data)), flags)
	if errno != 0 {
		return os.NewSyscallError("Msync", errno)
	}
	return nil
}
