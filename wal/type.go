package wal

import "fmt"

type Oid uint32
type TransactionId uint32
type PgCrc32c uint32
type TimeLineID uint32

type XLogRecPtr uint64

func (lsn XLogRecPtr) String() string {
	high := uint64(lsn) >> 32
	low := uint64(lsn) & 0xFFFFFFFF
	return fmt.Sprintf("%X/%08X", high, low)
}
