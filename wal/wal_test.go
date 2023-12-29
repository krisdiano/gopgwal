package wal

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPageLSN(t *testing.T) {
	lsn, err := PageLSN("000000010000000000000003", 64*1024*1024)
	assert.NoError(t, err)
	assert.EqualValues(t, 0x0C000000, lsn)
}

func TestWalName(t *testing.T) {
	cases := []struct {
		in  XLogRecPtr
		out string
	}{
		{0x0C000000, "000000010000000000000003"},
		{0x0C000000 - 1, "000000010000000000000002"},
	}
	for _, entry := range cases {
		t.Run(entry.out, func(t *testing.T) {
			walname, err := WalName(1, entry.in, 64*1024*1024)
			assert.NoError(t, err)
			assert.EqualValues(t, entry.out, walname)
		})
	}
}

type metadata struct {
	data        []byte
	startLSN    XLogRecPtr
	segmentSize uint32
	blockSize   uint32
}

var (
	md   *metadata
	once sync.Once
)

func Metadata() metadata {
	once.Do(func() {
		f, err := os.ReadFile("./testdata/000000010000000000000003")
		if err != nil {
			panic(err)
		}
		md = &metadata{data: f, startLSN: 0x0C000000, segmentSize: 64 * 1024 * 1024, blockSize: 32 * 1024}
	})
	return *md
}

func TestLongPageHeader(t *testing.T) {
	md := Metadata()

	longHeader, err := ReadXLogLongPageHeader(bytes.NewReader(md.data))
	assert.NoError(t, err)
	assert.EqualValues(t, md.segmentSize, longHeader.XlpSegSize)
	assert.EqualValues(t, md.blockSize, longHeader.XlpXLogBlcksz)
	assert.EqualValues(t, true, longHeader.Std.XlpInfo&XLP_ALL_FLAGS&XLP_LONG_HEADER != 0)
	assert.EqualValues(t, 1, longHeader.Std.XlpTli)
	assert.EqualValues(t, md.startLSN, longHeader.Std.XlpPageAddr)
}

func TestPageHeader(t *testing.T) {
	md := Metadata()

	assert.GreaterOrEqual(t, uint32(len(md.data)), md.blockSize+uint32(SizeofXLogPageHeaderData()))
	header, err := ReadXLogPageHeader(bytes.NewReader(md.data[md.blockSize:]))
	assert.NoError(t, err)
	assert.EqualValues(t, true, header.XlpInfo&XLP_ALL_FLAGS&XLP_LONG_HEADER == 0)
	assert.EqualValues(t, 1, header.XlpTli)
	assert.EqualValues(t, md.startLSN+XLogRecPtr(md.blockSize), header.XlpPageAddr)
}

func TestXLogRecord(t *testing.T) {
	reader, err := NewXLogReader("./testdata/000000010000000000000003", 8)
	assert.NoError(t, err)
	assert.NotNil(t, reader)

	var lastSucceedLSN XLogRecPtr
	for {
		record, err := reader.ReadRecord()
		if err != nil {
			fmt.Println(err)
			break
		}
		// t.Log(record.LSN, record.Hdr.XlTotlen)
		lastSucceedLSN = record.LSN
	}
	assert.Equal(t, XLogRecPtr(0x0C18F508), lastSucceedLSN)
}

func TestXLogSwitch(t *testing.T) {
	reader, err := NewXLogReader("./testdata/000000020000000000000005", 8)
	assert.NoError(t, err)
	assert.NotNil(t, reader)

	var lastSucceedLSN XLogRecPtr
	for {
		record, err := reader.ReadRecord()
		if err != nil {
			fmt.Println(err)
			break
		}
		// t.Log(record.LSN, record.Hdr.XlTotlen)
		lastSucceedLSN = record.LSN
	}
	assert.Equal(t, XLogRecPtr(0x14DC2390), lastSucceedLSN)
}
