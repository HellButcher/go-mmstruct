package mmf

import (
	"os"
	"unsafe"

	syscall "golang.org/x/sys/windows"
)

type MappedFile struct {
	data   []byte
	off    int
	file   *os.File
	handle syscall.Handle
}

func (self *MappedFile) mmap(size int) error {
	handle, err := syscall.CreateFileMapping(syscall.Handle(self.file.Fd()), nil, syscall.PAGE_READWRITE, 0, 0, nil) // 0,0 := total size of the file
	if err != nil {
		return os.NewSyscallError("CreateFileMapping", err)
	}
	ptr, err := syscall.MapViewOfFile(handle, syscall.FILE_MAP_WRITE, 0, 0, uintptr(size))
	if err != nil {
		syscall.CloseHandle(handle)
		return os.NewSyscallError("MapViewOfFile", err)
	}
	self.handle = handle
	self.data = (*[1<<31 - 1]byte)(unsafe.Pointer(ptr))[:size]
	return nil
}

func (self *MappedFile) munmap() error {
	if data := self.data; data != nil {
		self.data = nil
		if err := syscall.UnmapViewOfFile(uintptr(unsafe.Pointer(&data[0]))); err != nil {
			return os.NewSyscallError("UnmapViewOfFile", err)
		}
	}
	if handle := self.handle; handle != 0 && handle != ^syscall.Handle(0) {
		self.handle = 0
		if err := syscall.CloseHandle(handle); err != nil {
			return os.NewSyscallError("CloseHandle", err)
		}
	}
	return nil
}

func (self *MappedFile) sync(async bool) error {
	err := syscall.FlushViewOfFile(uintptr(unsafe.Pointer(&self.data[0])), uintptr(len(self.data)))
	if err != nil {
		return os.NewSyscallError("FlushViewOfFile", err)
	}
	if !async {
		if err := syscall.FlushFileBuffers(self.handle); err != nil {
			return os.NewSyscallError("FlushFileBuffers", err)
		}
	}
	return nil
}
