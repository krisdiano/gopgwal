package wal

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// func TestRecord(t *testing.T) {
// 	file := "./testdata/000000010000000300000001"
// 	f, err := os.Open(file)
// 	assert.NoError(t, err)
// 	defer f.Close()

// 	longHeader, err := ReadXLogLongPageHeader(f)
// 	assert.NoError(t, err)
// 	assert.NotNil(t, longHeader)
// 	assert.EqualValues(t, 0, longHeader.Std.XlpRemLen)

// 	records := []struct {
// 		Rmgr   string
// 		Total  uint32
// 		Xid    TransactionId
// 		Lsn    XLogRecPtr
// 		PreLsn XLogRecPtr
// 	}{
// 		{"Heap", 59, 756, 0x301000028, 0x300ffffc0},
// 		{"Heap", 59, 756, 0x301000068, 0x301000028},
// 		{"Heap", 59, 756, 0x3010000A8, 0x301000068},
// 		{"Heap", 59, 756, 0x3010000E8, 0x3010000A8},
// 		{"Heap", 59, 756, 0x301000128, 0x3010000E8},
// 	}
// 	for i := 0; i < 1; i++ {
// 		t.Run(fmt.Sprintf("Record %d", i+1), func(t *testing.T) {
// 			record, err := ReadXLogRecord(f)
// 			assert.NoError(t, err)
// 			assert.Equal(t, records[i].Rmgr, RmgrIdName(record.XlRmid))
// 			assert.Equal(t, records[i].Total, record.XlTotlen)
// 			assert.Equal(t, records[i].Xid, record.XlXid)
// 			// assert.Equal(t, records[i].Lsn&0x00ffffff, XLogRecPtr(offset))
// 			assert.Equal(t, records[i].PreLsn, record.XlPrev)
// 			// err = SkipRecord(f, record)
// 			// assert.NoError(t, err)

// 			var data []byte
// 			data = binary.BigEndian.AppendUint32(data, record.XlTotlen)
// 			data = binary.BigEndian.AppendUint32(data, uint32(record.XlXid))
// 			data = binary.BigEndian.AppendUint64(data, uint64(record.XlPrev))
// 			data = append(data, record.XlInfo)
// 			data = append(data, record.XlRmid)
// 			data = binary.BigEndian.AppendUint16(data, 0)
// 			crc := crc32.ChecksumIEEE(data)
// 			t.Log(crc, crc^0xffffffff, record.XlCrc)
// 		})
// 	}
// }

// func TestRecordHeader(t *testing.T) {
// 	file := "./testdata/000000010000000300000001"
// 	f, err := os.Open(file)
// 	assert.NoError(t, err)
// 	defer f.Close()

// 	longHeader, err := ReadXLogLongPageHeader(f)
// 	assert.NoError(t, err)
// 	assert.NotNil(t, longHeader)

// 	err = SkipRemainData(f, &longHeader.Std)
// 	assert.NoError(t, err)

// 	record, rpos, err := ReadXLogRecord(f)
// 	assert.NoError(t, err)
// 	id, err := ReadReferenceId(f)
// 	assert.NoError(t, err)
// 	assert.LessOrEqual(t, id, XLR_MAX_BLOCK_ID)
// 	bheader, err := ReadXLogRecordBlockHeader(f, id)
// 	assert.NoError(t, err)
// 	// assert.Equal(t, true, bheader.HasImage())
// 	// iheader, err := ReadXLogRecordBlockImageHeader(f)
// 	// assert.NoError(t, err)
// 	// assert.Equal(t, true, iheader.HasCompressed())
// 	// assert.Equal(t, true, iheader.HasHole())
// 	// _, err = ReadXLogRecordBlockCompressHeader(f)
// 	// assert.NoError(t, err)
// 	assert.Equal(t, true, bheader.HasFileNode())
// 	rfnode, err := ReadRelFileNode(f)
// 	assert.NoError(t, err)
// 	assert.Equal(t, "1663/16384/16397", fmt.Sprintf("%d/%d/%d", rfnode.SpcNode, rfnode.DbNode, rfnode.RelNode))
// 	blknum, err := ReadBlockNumber(f)
// 	assert.NoError(t, err)
// 	assert.Equal(t, BlockNumber(371658), blknum)
// 	id, err = ReadReferenceId(f)
// 	assert.NoError(t, err)
// 	// assert.Equal(t, XLR_BLOCK_ID_ORIGIN, id)
// 	// _, err = ReadRepOriginDummy(f, id)
// 	// assert.NoError(t, err)
// 	// id, err = ReadReferenceId(f)
// 	// assert.NoError(t, err)
// 	assert.Equal(t, XLR_BLOCK_ID_DATA_SHORT, id)
// 	dheader, err := ReadXLogRecordDataHeaderShort(f, id)
// 	assert.NoError(t, err)

// 	dataLen := bheader.DataLength + uint16(dheader.DataLength)
// 	pos, err := f.Seek(0, io.SeekCurrent)
// 	assert.NoError(t, err)
// 	assert.Equal(t, rpos+int64(record.XlTotlen), pos+int64(dataLen))

// 	err = Skip(f, int64(bheader.DataLength))
// 	assert.NoError(t, err)
// 	offsetBytes := make([]byte, 2)
// 	_, err = io.ReadFull(f, offsetBytes)
// 	assert.NoError(t, err)
// 	offset := *(*uint16)(unsafe.Pointer(&offsetBytes[0]))
// 	assert.Equal(t, uint16(63), offset)
// 	flagBytes := make([]byte, 2)
// 	_, err = io.ReadFull(f, flagBytes)
// 	assert.NoError(t, err)
// 	flag := *(*uint8)(unsafe.Pointer(&flagBytes[0]))
// 	assert.Equal(t, uint8(0), flag)
// }

func TestPanic(t *testing.T) {
	file := "./testdata/000000010000000000000003"
	content, err := os.ReadFile(file)
	assert.NoError(t, err)

	reader, err := NewXLogReader(8, 1024*1024*64, 32*1024, 0x0c000000, bytes.NewReader(content))
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

	// err = SkipRemainData(f, &longHeader.Std)
	// assert.NoError(t, err)

	// for {
	// 	record, offset, err := ReadXLogRecord(f)
	// 	assert.NoErrorf(t, err, "offset %d", offset)
	// 	if record.XlPrev.String() == "0/0c0004d8" {
	// 		t.Logf("%s %d, %+v", RmgrIdName(record.XlRmid), offset, record)
	// 		break
	// 	}
	// 	pos, err := f.Seek(0, io.SeekCurrent)
	// 	remain := int64(record.XlTotlen - 24)
	// 	bk := int64(1024 * 8)
	// 	readable := bk - pos%bk
	// 	for readable < remain {
	// 		remain -= readable
	// 		_, err := f.Seek(remain+1, io.SeekCurrent)
	// 		assert.NoError(t, err)
	// 		header, err := ReadXLogPageHeader(f)
	// 		assert.NoError(t, err)
	// 		readable = int64(header.XlpRemLen)
	// 	}
	// 	assert.NoError(t, err)
	// 	err = SkipRecord(f, record)
	// 	assert.NoError(t, err)
	// }
}