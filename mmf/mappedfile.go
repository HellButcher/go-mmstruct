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

const DefaultMode os.FileMode = 0666

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
	f, err := os.OpenFile(filename, createFlags, DefaultMode)
	if err != nil {
		return nil, err
	}
	err = f.Truncate(size)
	if err != nil {
		f.Close()
		return nil, err
	}
	mf, err := openMappedFile(f, int(size))
	if err != nil {
		f.Close()
		return nil, err
	}
	return mf, nil
}

// OpenMappedFile opens an existing file and maps it to memory. The mapped
// memory is Readable and Writeable. The operating system will write the changes
// to the file at some point later. When multiple processes open the same file,
// the mapped memory for will be shared between the processes.
// It returns an error, if any.
func OpenMappedFile(filename string) (*MappedFile, error) {
	f, err := os.OpenFile(filename, openFlags, DefaultMode)
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
	mf, err := openMappedFile(f, int(size))
	if err != nil {
		f.Close()
		return nil, err
	}
	return mf, nil
}

func openMappedFile(file *os.File, size int) (*MappedFile, error) {
	mf := &MappedFile{file: file}
	if err := mf.mmap(size); err != nil {
		return nil, err
	}
	runtime.SetFinalizer(mf, (*MappedFile).Close)
	return mf, nil
}

// Close unmaps the mapped memory and closes the File.
// It returns an error, if any.
func (mf *MappedFile) Close() error {
	if mf == nil {
		return nil
	}
	if err := mf.munmap(); err != nil {
		return err
	}
	if file := mf.file; file != nil {
		mf.file = nil
		if err := file.Close(); err != nil {
			return err
		}
	}
	runtime.SetFinalizer(mf, nil)
	return nil
}

// Sync tells the operating system to write the changes back to the file soon.
// It returns an error, if any.
func (mf *MappedFile) Sync() error {
	if mf == nil || mf.data == nil || mf.file == nil {
		return errors.New("MappedFile: closed")
	}
	return mf.sync(false)
}

// Truncate changes the size of the file and the mapped memory area.
// The (virtual-)address of the mapped memory area will possibly change.
// It returns an error, if any.
func (mf *MappedFile) Truncate(size int64) error {
	if mf == nil || mf.data == nil || mf.file == nil {
		return errors.New("MappedFile: closed")
	}
	if size < 0 {
		return fmt.Errorf("MappedFile: requested file size is negative")
	}
	if size != int64(int(size)) {
		return fmt.Errorf("MappedFile: requested file size is too large")
	}
	if err := mf.munmap(); err != nil {
		return err
	}
	if err := mf.file.Truncate(size); err != nil {
		return err
	}
	if err := mf.mmap(int(size)); err != nil {
		return err
	}
	return nil
}

// Fd returns the file descriptor handle referencing the open file.
// The file descriptor is valid only until mf.Close is called or mf is
// garbage collected.
func (mf *MappedFile) Fd() uintptr {
	if mf != nil && mf.file != nil {
		return mf.file.Fd()
	} else {
		return ^(uintptr(0))
	}
}

// Name returns the name of the file as presented to CreateMappedFile or
// OpenMappedFile.
func (mf *MappedFile) Name() string {
	if mf != nil && mf.file != nil {
		return mf.file.Name()
	} else {
		return ""
	}
}

// Size returns the total size of the mapped memory.
// mf.Size() == len(mf.Bytes()).
func (mf *MappedFile) Size() int {
	if mf != nil && mf.data != nil {
		return len(mf.data)
	} else {
		return 0
	}
}

// Len returns the number of bytes of the unread portion of the memory.
// mf.Offset() + mf.Len() == mf.Size().
func (mf *MappedFile) Len() int {
	if mf != nil && mf.data != nil {
		return len(mf.data) - mf.off
	} else {
		return 0
	}
}

// Bytes returns a slice to the mapped memory area.
// The slice is valid only until mf.Close or mf.Truncate is called or mf
// is garbage collected.
func (mf *MappedFile) Bytes() []byte {
	if mf != nil {
		return mf.data
	} else {
		return nil
	}
}

// Next returns a slice containing the next n bytes in the mapped memory and
// advances the current position as if the bytes had been returned by Read.
// If there are fewer than n bytes in the buffer, Next only returns the subset
// until the end of the mapped memory.
// The slice is valid only until mf.Close or mf.Truncate is called or mf
// is garbage collected.
func (mf *MappedFile) Next(n int) []byte {
	if mf == nil || mf.data == nil {
		return nil
	}
	m := mf.Len()
	if n > m {
		n = m
	}
	data := mf.data[mf.off : mf.off+n]
	mf.off += n
	return data
}

// Read reads up to len(b) bytes from the mapped memory.
// It returns the number of bytes read and an error, if any.
// EOF is signaled by a zero count with err set to io.EOF.
func (mf *MappedFile) Read(p []byte) (int, error) {
	if mf == nil || mf.data == nil {
		return 0, errors.New("MappedFile: closed")
	}
	if len(p) == 0 {
		return 0, nil
	}
	if mf.off >= len(mf.data) {
		return 0, io.EOF
	}
	n := copy(p, mf.data[mf.off:])
	mf.off += n
	return n, nil
}

// ReadByte reads and returns the next byte from the mapped memory.
// If no byte is available, it returns error io.EOF.
func (mf *MappedFile) ReadByte() (byte, error) {
	if mf == nil || mf.data == nil {
		return 0, errors.New("MappedFile: closed")
	}
	if mf.off >= len(mf.data) {
		return 0, io.EOF
	}
	c := mf.data[mf.off]
	mf.off++
	return c, nil
}

// Write writes up to len(b) bytes to the mapped memory.
// It returns the number of bytes written and an error, if any.
// Write returns a non-nil error when n != len(b).
// The file/memory doesn't grow automatically.
// EOF is signaled by a zero count with err set to io.EOF.
func (mf *MappedFile) Write(p []byte) (int, error) {
	if mf == nil || mf.data == nil {
		return 0, errors.New("MappedFile: closed")
	}
	if len(p) == 0 {
		return 0, nil
	}
	if mf.off >= len(mf.data) {
		return 0, io.EOF
	}
	n := copy(mf.data[mf.off:], p)
	mf.off += n
	return n, nil
}

// WriteByte writes the next byte to the mapped memory.
// If the end of the mapped memory area is reached, it returns error io.EOF.
func (mf *MappedFile) WriteByte(c byte) error {
	if mf == nil || mf.data == nil {
		return errors.New("MappedFile: closed")
	}
	if mf.off >= len(mf.data) {
		return io.EOF
	}
	mf.data[mf.off] = c
	mf.off++
	return nil
}

// Offset returns the current position in the mapped memory. The next call to
// Read or Write will start at this position.
func (mf *MappedFile) Offset() int {
	if mf == nil || mf.data == nil {
		return 0
	} else {
		return mf.off
	}
}

// Seek sets the offset for the next Read or Write on mapped memory to offset,
// interpreted according to whence:
//   0 means relative to the origin of the file,
//   1 means relative to the current offset,
//   and 2 means relative to the end.
// It returns the new offset and an error, if any.
func (mf *MappedFile) Seek(offset int64, whence int) (int64, error) {
	if mf == nil || mf.data == nil {
		return 0, errors.New("MappedFile: closed")
	}
	if whence == SeekStart {
		// nothing to do
	} else if whence == SeekCurrent {
		offset = int64(mf.off) + offset
	} else if whence == SeekEnd {
		offset = int64(len(mf.data)) + offset
	} else {
		return int64(mf.off), fmt.Errorf("MappedFile: unknown seek operation %d", whence)
	}
	if offset < 0 || offset > int64(len(mf.data)) {
		return int64(mf.off), fmt.Errorf("MappedFile: out of bounds")
	}
	mf.off = int(offset)
	return offset, nil
}

// ReadAt reads up to len(b) bytes from the mapped memory starting at byte offset off.
// It returns the number of bytes read and the error, if any.
// ReadAt always returns a non-nil error when n < len(b).
// At end of file, that error is io.EOF.
func (mf *MappedFile) ReadAt(b []byte, off int64) (int, error) {
	if mf == nil || mf.data == nil {
		return 0, errors.New("MappedFile: closed")
	}
	if off < 0 || int64(len(mf.data)) < off {
		return 0, fmt.Errorf("MappedFile: invalid ReadAt offset %d", off)
	}
	n := copy(b, mf.data[off:])
	if n < len(b) {
		return n, io.EOF
	}
	return n, nil
}

// WriteAt writes up to len(b) bytes to the mapped memory starting at byte offset off.
// It returns the number of bytes written and an error, if any.
// ReadAt always returns a non-nil error when n < len(b).
// At end of file, that error is io.EOF.
func (mf *MappedFile) WriteAt(b []byte, off int64) (int, error) {
	if mf == nil || mf.data == nil {
		return 0, errors.New("MappedFile: closed")
	}
	if off < 0 || int64(len(mf.data)) < off {
		return 0, fmt.Errorf("MappedFile: invalid WriteAt offset %d", off)
	}
	n := copy(mf.data[off:], b)
	if n < len(b) {
		return n, io.EOF
	}
	return n, nil
}
