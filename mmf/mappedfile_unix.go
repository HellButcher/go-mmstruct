// +build !windows

package mmf

import (
	"os"
	"unsafe"

	syscall "golang.org/x/sys/unix"
)

// MappedFile is a struct that defines an open memory mapped file
type MappedFile struct {
	data []byte
	off  int
	file *os.File
}

func (mf *MappedFile) mmap(size int) error {
	var err error
	mf.data, err = syscall.Mmap(int(mf.file.Fd()), 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return os.NewSyscallError("Mmap", err)
	}
	return nil
}

func (mf *MappedFile) munmap() error {
	if data := mf.data; data != nil {
		mf.data = nil
		if err := syscall.Munmap(data); err != nil {
			return os.NewSyscallError("Munmap", err)
		}
	}
	return nil
}

func (mf *MappedFile) sync(async bool) error {
	var flags uintptr
	if async {
		flags = syscall.MS_ASYNC
	} else {
		flags = syscall.MS_SYNC
	}
	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC, uintptr(unsafe.Pointer(&mf.data[0])), uintptr(len(mf.data)), flags)
	if errno != 0 {
		return os.NewSyscallError("Msync", errno)
	}
	return nil
}
