package wal

import (
	"bytes"
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
	assert.EqualValues(t, md.startLSN, longHeader.Std.XlpPagedddr)
}

func TestPageHeader(t *testing.T) {
	md := Metadata()

	assert.GreaterOrEqual(t, uint32(len(md.data)), md.blockSize+uint32(SizeofXLogPageHeaderData()))
	header, err := ReadXLogPageHeader(bytes.NewReader(md.data[md.blockSize:]))
	assert.NoError(t, err)
	assert.EqualValues(t, true, header.XlpInfo&XLP_ALL_FLAGS&XLP_LONG_HEADER == 0)
	assert.EqualValues(t, 1, header.XlpTli)
	assert.EqualValues(t, md.startLSN+XLogRecPtr(md.blockSize), header.XlpPagedddr)
}

func TestXLogRecord(t *testing.T) {
	reader, err := NewXLogReader(8, md.segmentSize, md.blockSize, md.startLSN, bytes.NewReader(md.data))
	assert.NoError(t, err)
	_, err = reader.FindFirstRecord(reader.LSN())
	assert.NoError(t, err)

	var lastSucceedLSN XLogRecPtr
	for {
		record, err := reader.ReadRecord()
		if err != nil {
			break
		}
		lastSucceedLSN = record.LSN
		_, err = reader.FindNextRecord()
		if err != nil {
			break
		}
	}
	assert.Equal(t, lastSucceedLSN, XLogRecPtr(0x0C18F508))
}
