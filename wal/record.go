package wal

import (
	"io"
	"unsafe"
)

type TransactionId uint32
type PgCrc32c uint32

type XLogRecord struct {
	XlTotlen uint32
	XlXid    TransactionId
	XlPrev   XLogRecPtr
	XlInfo   uint8
	XlRmid   RmgrId
	padding  uint16
	XlCrc    PgCrc32c
}

func ReadXLogRecord(reader io.ReadSeeker) (*XLogRecord, int64, error) {
	var (
		record  XLogRecord
		content = make([]byte, unsafe.Sizeof(record))
	)

	offset, err := reader.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, 0, err
	}

	_, err = io.ReadFull(reader, content)
	if err != nil {
		return nil, 0, err
	}
	ptr := (*XLogRecord)(unsafe.Pointer(&content[0]))
	record = *ptr
	return &record, offset, nil
}

func SkipRecord(seeker io.Seeker, record *XLogRecord) error {
	remainLen := record.XlTotlen - uint32(unsafe.Sizeof(*record))
	_, err := seeker.Seek(int64(remainLen), io.SeekCurrent)
	if err != nil {
		return err
	}
	return Align(seeker)
}
