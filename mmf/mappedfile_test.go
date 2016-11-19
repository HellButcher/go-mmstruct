package mmf_test

import (
	"os"
	"testing"

	. "github.com/hellbutcher/go-mmstruct/mmf"
)

func close(mf *MappedFile, t *testing.T) {
	if err := mf.Close(); err != nil {
		t.Fatal("Error while closing mapped file:", err)
	}
}

func TestCreateAndOpenMappedFile(t *testing.T) {
	defer os.Remove("test1.tmp")
	{
		// create a mapped file
		mf, err := CreateMappedFile("test1.tmp", 4096)
		if err != nil {
			t.Fatal("Error while creating mapped file:", err)
		}
		defer close(mf, t)

		if n := mf.Name(); n != "test1.tmp" {
			t.Error("name mismatch. got", n)
		}
		if s := mf.Size(); s != 4096 {
			t.Error("size mismatch. expected 4096, got", s)
		}
		if s := mf.Len(); s != 4096 {
			t.Error("len mismatch. expected 4096, got", s)
		}

		// write using WriteAt
		n, err := mf.WriteAt([]byte("Test123456"), 100)
		if err != nil {
			t.Fatal("Error while writing to mapped file:", err)
		}
		if n != 10 {
			t.Error("unexpected write count. expected 10, got", n)
		}
		// Write by accessing the mapped memory
		n = copy(mf.Bytes()[200:], []byte("ABCDE"))
		if n != 5 {
			t.Error("unexpected write count. expected 5, got", n)
		}
		close(mf, t)
	}
	{
		// open the file again
		mf, err := OpenMappedFile("test1.tmp")
		if err != nil {
			t.Fatal("Error while opening mapped file:", err)
		}
		defer close(mf, t)
		if s := mf.Size(); s != 4096 {
			t.Error("size mismatch. expected 4096, got", s)
		}

		// read using ReadAt
		var data1 [5]byte
		n, err := mf.ReadAt(data1[:], 200)
		if err != nil {
			t.Fatal("Error while reading from mapped file:", err)
		}
		if n != 5 {
			t.Error("unexpected read count. expected 5, got", n)
		}
		if string(data1[:]) != "ABCDE" {
			t.Error("expected ABCDE, got", data1)
		}
		// Read by accessing the mapped memory
		var data2 [10]byte
		n = copy(data2[:], mf.Bytes()[100:])
		if n != 10 {
			t.Error("unexpected read count. expected 10, got", n)
		}
		if string(data2[:]) != "Test123456" {
			t.Error("expected Test123456, got", data2)
		}
		// now truncate
		err = mf.Truncate(8192)
		if err != nil {
			t.Fatal("Error while truncating mapped file:", err)
		}
		// old data is still the same
		n = copy(data2[:], mf.Bytes()[100:])
		if n != 10 {
			t.Error("unexpected read count. expected 10, got", n)
		}
		if string(data2[:]) != "Test123456" {
			t.Error("expected Test123456, got", data2)
		}
		if s := mf.Size(); s != 8192 {
			t.Error("size mismatch. expected 8192, got", s)
		}
		// write to the truncated region
		n = copy(mf.Bytes()[6000:], []byte("ABCDE"))
		if n != 5 {
			t.Error("unexpected write count. expected 5, got", n)
		}
		close(mf, t)
	}
	{
		// open the truncated file again
		mf, err := OpenMappedFile("test1.tmp")
		if err != nil {
			t.Fatal("Error while opening mapped file:", err)
		}
		defer close(mf, t)
		if s := mf.Size(); s != 8192 {
			t.Error("size mismatch. expected 8192, got", s)
		}
		// Read by accessing the mapped memory
		var data1 [5]byte
		n := copy(data1[:], mf.Bytes()[6000:])
		if n != 5 {
			t.Error("unexpected read count. expected 5, got", n)
		}
		if string(data1[:]) != "ABCDE" {
			t.Error("expected ABCDE, got", data1)
		}
		close(mf, t)
	}
}
