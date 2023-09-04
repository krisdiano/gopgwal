package wal

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRecord(t *testing.T) {
	file := "./testdata/000000010000000300000001"
	f, err := os.Open(file)
	assert.NoError(t, err)
	defer f.Close()

	longHeader, err := ReadXLogLongPageHeader(f)
	assert.NoError(t, err)
	assert.NotNil(t, longHeader)

	err = SkipRemainData(f, &longHeader.Std)
	assert.NoError(t, err)

	records := []struct {
		Rmgr   string
		Total  uint32
		Xid    TransactionId
		Lsn    XLogRecPtr
		PreLsn XLogRecPtr
	}{
		{"Heap", 59, 756, 0x301000028, 0x300ffffc0},
		{"Heap", 59, 756, 0x301000068, 0x301000028},
		{"Heap", 59, 756, 0x3010000A8, 0x301000068},
		{"Heap", 59, 756, 0x3010000E8, 0x3010000A8},
		{"Heap", 59, 756, 0x301000128, 0x3010000E8},
	}
	for i := 0; i < 5; i++ {
		t.Run(fmt.Sprintf("Record %d", i+1), func(t *testing.T) {
			record, offset, err := ReadXLogRecord(f)
			assert.NoError(t, err)
			assert.Equal(t, records[i].Rmgr, RmgrIdName(record.XlRmid))
			assert.Equal(t, records[i].Total, record.XlTotlen)
			assert.Equal(t, records[i].Xid, record.XlXid)
			assert.Equal(t, records[i].Lsn&0x00ffffff, XLogRecPtr(offset))
			assert.Equal(t, records[i].PreLsn, record.XlPrev)
			err = SkipRecord(f, record)
			assert.NoError(t, err)
		})
	}
}
