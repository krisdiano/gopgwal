package wal

import "fmt"

func PageLSN(walname string, segmentSize uint32) (XLogRecPtr, error) {
	var (
		tli    TimeLineID
		logId  uint64
		logSeq uint64
	)
	_, err := fmt.Sscanf(walname, "%08X%08X%08X", &tli, &logId, &logSeq)
	if err != nil {
		return 0, err
	}
	return XLogRecPtr((logId << 32) | logSeq*uint64(segmentSize)), nil
}

func WalName(tli TimeLineID, lsn XLogRecPtr, segmentSize uint32) (string, error) {
	mask := uint64(0xFFFFFFFF)
	logId := uint64(lsn) >> 32
	logSeq := uint64(lsn) & mask
	return fmt.Sprintf("%08X%08X%08X", tli, logId, logSeq/uint64(segmentSize)), nil
}
