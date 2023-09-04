package wal

import (
	"os"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestAlignment(t *testing.T) {
	var (
		a XLogPageHeaderData
		b XLogLongPageHeaderData
	)

	assert.Equal(t, int(unsafe.Sizeof(a)), 24)
	assert.Equal(t, int(unsafe.Sizeof(b)), 40)
}

func TestPage(t *testing.T) {
	files := []string{
		"./testdata/000000010000000300000001",
		"./testdata/000000010000000300000002",
	}

	magic := make(map[uint16]struct{})
	sysid := make(map[uint64]struct{})
	segsize := make(map[uint32]struct{})
	blcksz := make(map[uint32]struct{})
	for _, file := range files {
		t.Run(file, func(t *testing.T) {
			f, err := os.Open(file)
			assert.NoError(t, err)
			defer f.Close()

			longHeader, err := ReadXLogLongPageHeader(f)
			assert.NoError(t, err)
			assert.NotNil(t, longHeader)

			magic[longHeader.Std.XlpMagic] = struct{}{}
			sysid[longHeader.XlpSysid] = struct{}{}
			segsize[longHeader.XlpSegSize] = struct{}{}
			blcksz[longHeader.XlpXLogBlcksz] = struct{}{}

			_, err = f.Seek(1024*8, 0)
			assert.NoError(t, err)

			header, err := ReadXLogPageHeader(f)
			assert.NoError(t, err)
			assert.NotNil(t, header)
			magic[header.XlpMagic] = struct{}{}

			t.Logf("%v", longHeader)
		})
	}
	assert.Len(t, magic, 1)
	assert.Len(t, sysid, 1)
	assert.Len(t, segsize, 1)
	assert.Len(t, blcksz, 1)
}

func TestLsn(t *testing.T) {
	tcase := []struct {
		in  XLogRecPtr
		out string
	}{
		{0x301000028, "3/01000028"},
		{0x301000268, "3/01000268"},
	}

	for _, item := range tcase {
		t.Run(item.out, func(t *testing.T) {
			assert.Equal(t, item.out, item.in.String())
		})
	}
}
