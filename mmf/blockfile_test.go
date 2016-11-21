package mmf_test

import (
	"os"
	"testing"

	. "github.com/HellButcher/go-mmstruct/mmf"
)

func closeBF(bf *BlockFile, t *testing.T) {
	if err := bf.Close(); err != nil {
		t.Fatal("Error while closing block file:", err)
	}
}

func TestCreateAllocateANdFreeBF(t *testing.T) {
	defer os.Remove("bftest.tmp")
	bf, err := CreateBlockFileWithSize("bftest.tmp", 32)
	if err != nil {
		t.Fatal("Error while creating block file:", err)
	}
	defer closeBF(bf, t)

	block, err := bf.AllocateBlock()
	if err != nil {
		t.Fatal("Error while allocatin block 1", err)
	}
	if block != 1 {
		t.Error("unexpected block index. expected 1, got ", block)
	}

	block, err = bf.AllocateBlock()
	if err != nil {
		t.Fatal("Error while allocatin block 2", err)
	}
	if block != 2 {
		t.Error("unexpected block index. expected 2, got ", block)
	}

	block, err = bf.AllocateBlock()
	if err != nil {
		t.Fatal("Error while allocatin block 3", err)
	}
	if block != 3 {
		t.Error("unexpected block index. expected 3, got ", block)
	}

	err = bf.FreeBlock(2)
	if err != nil {
		t.Fatal("Error while freeing block 2", err)
	}

	err = bf.FreeBlock(1)
	if err != nil {
		t.Fatal("Error while freeing block 1", err)
	}

	block, err = bf.AllocateBlock()
	if err != nil {
		t.Fatal("Error while allocatin block 1.1", err)
	}
	if block != 1 {
		t.Error("unexpected block index. expected 1, got ", block)
	}

	block, err = bf.AllocateBlock()
	if err != nil {
		t.Fatal("Error while allocatin block 2.1", err)
	}
	if block != 2 {
		t.Error("unexpected block index. expected 2, got ", block)
	}

	for n := 4; n < 40; n++ {
		block, err = bf.AllocateBlock()
		if err != nil {
			t.Fatal("Error while allocatin block", n, err)
		}
		if block != n {
			t.Error("unexpected block index. expected ", n, ", got", block)
		}
	}

	for n := 4; n < 20; n++ {
		err = bf.FreeBlock(n)
		if err != nil {
			t.Fatal("Error while freeing block", n, err)
		}
	}
	for n := 30; n < 40; n++ {
		err = bf.FreeBlock(n)
		if err != nil {
			t.Fatal("Error while freeing block", n, err)
		}
	}
}
