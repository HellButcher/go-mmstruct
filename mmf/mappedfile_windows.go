package mmf

import (
	"os"
	"unsafe"

	syscall "golang.org/x/sys/windows"
)

// MappedFile is a struct that defines an open memory mapped file
type MappedFile struct {
	data   []byte
	off    int
	file   *os.File
	handle syscall.Handle
}

func (mf *MappedFile) mmap(size int) error {
	handle, err := syscall.CreateFileMapping(syscall.Handle(mf.file.Fd()), nil, syscall.PAGE_READWRITE, 0, 0, nil) // 0,0 := total size of the file
	if err != nil {
		return os.NewSyscallError("CreateFileMapping", err)
	}
	ptr, err := syscall.MapViewOfFile(handle, syscall.FILE_MAP_WRITE, 0, 0, uintptr(size))
	if err != nil {
		syscall.CloseHandle(handle)
		return os.NewSyscallError("MapViewOfFile", err)
	}
	mf.handle = handle
	mf.data = (*[1<<31 - 1]byte)(unsafe.Pointer(ptr))[:size]
	return nil
}

func (mf *MappedFile) munmap() error {
	if data := mf.data; data != nil {
		mf.data = nil
		if err := syscall.UnmapViewOfFile(uintptr(unsafe.Pointer(&data[0]))); err != nil {
			return os.NewSyscallError("UnmapViewOfFile", err)
		}
	}
	if handle := mf.handle; handle != 0 && handle != ^syscall.Handle(0) {
		mf.handle = 0
		if err := syscall.CloseHandle(handle); err != nil {
			return os.NewSyscallError("CloseHandle", err)
		}
	}
	return nil
}

func (mf *MappedFile) sync(async bool) error {
	err := syscall.FlushViewOfFile(uintptr(unsafe.Pointer(&mf.data[0])), uintptr(len(mf.data)))
	if err != nil {
		return os.NewSyscallError("FlushViewOfFile", err)
	}
	if !async {
		if err := syscall.FlushFileBuffers(mf.handle); err != nil {
			return os.NewSyscallError("FlushFileBuffers", err)
		}
	}
	return nil
}
