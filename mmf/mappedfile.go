package mmf

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
)

const (
	SeekStart   = io.SeekStart   // seek relative to the origin of the file
	SeekCurrent = io.SeekCurrent // seek relative to the current offset
	SeekEnd     = io.SeekEnd     // seek relative to the end
)

const DEFAULT_MODE os.FileMode = 0666

// CreateMappedFile creates a new file (or replaces an existing one) with the
// given initial size. The file is then mapped to memory. The mapped memory is
// Readable and Writeable. The operating system will write the changes to the
// file at some point later. When multiple processes open the same file,
// the mapped memory for will be shared between the processes.
// It returns an error, if any.
func CreateMappedFile(filename string, size int64) (*MappedFile, error) {
	if size < 0 {
		return nil, fmt.Errorf("MappedFile: requested file size is negative")
	}
	if size != int64(int(size)) {
		return nil, fmt.Errorf("MappedFile: requested file size is too large")
	}
	f, err := os.OpenFile(filename, CREATE_FLAGS, DEFAULT_MODE)
	if err != nil {
		return nil, err
	}
	err = f.Truncate(size)
	if err != nil {
		f.Close()
		return nil, err
	}
	self, err := openMappedFile(f, int(size))
	if err != nil {
		f.Close()
		return nil, err
	}
	return self, nil
}

// OpenMappedFile opens an existing file and maps it to memory. The mapped
// memory is Readable and Writeable. The operating system will write the changes
// to the file at some point later. When multiple processes open the same file,
// the mapped memory for will be shared between the processes.
// It returns an error, if any.
func OpenMappedFile(filename string) (*MappedFile, error) {
	f, err := os.OpenFile(filename, OPEN_FLAGS, DEFAULT_MODE)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	size := fi.Size()
	if size < 0 {
		f.Close()
		return nil, fmt.Errorf("MappedFile: file %q has negative size", filename)
	}
	if size != int64(int(size)) {
		f.Close()
		return nil, fmt.Errorf("MappedFile: file %q is too large", filename)
	}
	self, err := openMappedFile(f, int(size))
	if err != nil {
		f.Close()
		return nil, err
	}
	return self, nil
}

func openMappedFile(file *os.File, size int) (*MappedFile, error) {
	self := &MappedFile{file: file}
	if err := self.mmap(size); err != nil {
		return nil, err
	}
	runtime.SetFinalizer(self, (*MappedFile).Close)
	return self, nil
}

// Close unmaps the mapped memory and closes the File.
// It returns an error, if any.
func (self *MappedFile) Close() error {
	if self == nil {
		return nil
	}
	if err := self.munmap(); err != nil {
		return err
	}
	if file := self.file; file != nil {
		self.file = nil
		if err := file.Close(); err != nil {
			return err
		}
	}
	runtime.SetFinalizer(self, nil)
	return nil
}

// Sync tells the operating system to write the changes back to the file soon.
// It returns an error, if any.
func (self *MappedFile) Sync() error {
	if self == nil || self.data == nil || self.file == nil {
		return errors.New("MappedFile: closed")
	}
	return self.sync(false)
}

// Truncate changes the size of the file and the mapped memory area.
// The (virtual-)address of the mapped memory area will possibly change.
// It returns an error, if any.
func (self *MappedFile) Truncate(size int64) error {
	if self == nil || self.data == nil || self.file == nil {
		return errors.New("MappedFile: closed")
	}
	if size < 0 {
		return fmt.Errorf("MappedFile: requested file size is negative")
	}
	if size != int64(int(size)) {
		return fmt.Errorf("MappedFile: requested file size is too large")
	}
	if err := self.munmap(); err != nil {
		return err
	}
	if err := self.file.Truncate(size); err != nil {
		return err
	}
	if err := self.mmap(int(size)); err != nil {
		return err
	}
	return nil
}

// Fd returns the file descriptor handle referencing the open file.
// The file descriptor is valid only until self.Close is called or self is
// garbage collected.
func (self *MappedFile) Fd() uintptr {
	if self != nil && self.file != nil {
		return self.file.Fd()
	} else {
		return ^(uintptr(0))
	}
}

// Name returns the name of the file as presented to CreateMappedFile or
// OpenMappedFile.
func (self *MappedFile) Name() string {
	if self != nil && self.file != nil {
		return self.file.Name()
	} else {
		return ""
	}
}

// Size returns the total size of the mapped memory.
// self.Size() == len(self.Bytes()).
func (self *MappedFile) Size() int {
	if self != nil && self.data != nil {
		return len(self.data)
	} else {
		return 0
	}
}

// Len returns the number of bytes of the unread portion of the memory.
// self.Offset() + self.Len() == self.Size().
func (self *MappedFile) Len() int {
	if self != nil && self.data != nil {
		return len(self.data) - self.off
	} else {
		return 0
	}
}

// Bytes returns a slice to the mapped memory area.
// The slice is valid only until self.Close or self.Truncate is called or self
// is garbage collected.
func (self *MappedFile) Bytes() []byte {
	if self != nil {
		return self.data
	} else {
		return nil
	}
}

// Next returns a slice containing the next n bytes in the mapped memory and
// advances the current position as if the bytes had been returned by Read.
// If there are fewer than n bytes in the buffer, Next only returns the subset
// until the end of the mapped memory.
// The slice is valid only until self.Close or self.Truncate is called or self
// is garbage collected.
func (self *MappedFile) Next(n int) []byte {
	if self == nil || self.data == nil {
		return nil
	}
	m := self.Len()
	if n > m {
		n = m
	}
	data := self.data[self.off : self.off+n]
	self.off += n
	return data
}

// Read reads up to len(b) bytes from the mapped memory.
// It returns the number of bytes read and an error, if any.
// EOF is signaled by a zero count with err set to io.EOF.
func (self *MappedFile) Read(p []byte) (int, error) {
	if self == nil || self.data == nil {
		return 0, errors.New("MappedFile: closed")
	}
	if len(p) == 0 {
		return 0, nil
	}
	if self.off >= len(self.data) {
		return 0, io.EOF
	}
	n := copy(p, self.data[self.off:])
	self.off += n
	return n, nil
}

// ReadByte reads and returns the next byte from the mapped memory.
// If no byte is available, it returns error io.EOF.
func (self *MappedFile) ReadByte() (byte, error) {
	if self == nil || self.data == nil {
		return 0, errors.New("MappedFile: closed")
	}
	if self.off >= len(self.data) {
		return 0, io.EOF
	}
	c := self.data[self.off]
	self.off++
	return c, nil
}

// Write writes up to len(b) bytes to the mapped memory.
// It returns the number of bytes written and an error, if any.
// Write returns a non-nil error when n != len(b).
// The file/memory doesn't grow automatically.
// EOF is signaled by a zero count with err set to io.EOF.
func (self *MappedFile) Write(p []byte) (int, error) {
	if self == nil || self.data == nil {
		return 0, errors.New("MappedFile: closed")
	}
	if len(p) == 0 {
		return 0, nil
	}
	if self.off >= len(self.data) {
		return 0, io.EOF
	}
	n := copy(self.data[self.off:], p)
	self.off += n
	return n, nil
}

// WriteByte writes the next byte to the mapped memory.
// If the end of the mapped memory area is reached, it returns error io.EOF.
func (self *MappedFile) WriteByte(c byte) error {
	if self == nil || self.data == nil {
		return errors.New("MappedFile: closed")
	}
	if self.off >= len(self.data) {
		return io.EOF
	}
	self.data[self.off] = c
	self.off++
	return nil
}

// Offset returns the current position in the mapped memory. The next call to
// Read or Write will start at this position.
func (self *MappedFile) Offset() int {
	if self == nil || self.data == nil {
		return 0
	} else {
		return self.off
	}
}

// Seek sets the offset for the next Read or Write on mapped memory to offset,
// interpreted according to whence:
//   0 means relative to the origin of the file,
//   1 means relative to the current offset,
//   and 2 means relative to the end.
// It returns the new offset and an error, if any.
func (self *MappedFile) Seek(offset int64, whence int) (int64, error) {
	if self == nil || self.data == nil {
		return 0, errors.New("MappedFile: closed")
	}
	if whence == SeekStart {
		// nothing to do
	} else if whence == SeekCurrent {
		offset = int64(self.off) + offset
	} else if whence == SeekEnd {
		offset = int64(len(self.data)) + offset
	} else {
		return int64(self.off), fmt.Errorf("MappedFile: unknown seek operation %d", whence)
	}
	if offset < 0 || offset > int64(len(self.data)) {
		return int64(self.off), fmt.Errorf("MappedFile: out of bounds")
	}
	self.off = int(offset)
	return offset, nil
}

// ReadAt reads up to len(b) bytes from the mapped memory starting at byte offset off.
// It returns the number of bytes read and the error, if any.
// ReadAt always returns a non-nil error when n < len(b).
// At end of file, that error is io.EOF.
func (self *MappedFile) ReadAt(b []byte, off int64) (int, error) {
	if self == nil || self.data == nil {
		return 0, errors.New("MappedFile: closed")
	}
	if off < 0 || int64(len(self.data)) < off {
		return 0, fmt.Errorf("MappedFile: invalid ReadAt offset %d", off)
	}
	n := copy(b, self.data[off:])
	if n < len(b) {
		return n, io.EOF
	}
	return n, nil
}

// WriteAt writes up to len(b) bytes to the mapped memory starting at byte offset off.
// It returns the number of bytes written and an error, if any.
// ReadAt always returns a non-nil error when n < len(b).
// At end of file, that error is io.EOF.
func (self *MappedFile) WriteAt(b []byte, off int64) (int, error) {
	if self == nil || self.data == nil {
		return 0, errors.New("MappedFile: closed")
	}
	if off < 0 || int64(len(self.data)) < off {
		return 0, fmt.Errorf("MappedFile: invalid WriteAt offset %d", off)
	}
	n := copy(self.data[off:], b)
	if n < len(b) {
		return n, io.EOF
	}
	return n, nil
}
