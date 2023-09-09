package wal

import "bufio"

type WalBufReader struct {
	// point to the reader
	lsn XLogRecPtr
	*bufio.Reader
}

func NewWalBufReader(lsn XLogRecPtr, reader *bufio.Reader) *WalBufReader {
	return &WalBufReader{
		lsn:    lsn,
		Reader: reader,
	}
}

func (wbr *WalBufReader) Read(p []byte) (int, error) {
	n, err := wbr.Reader.Read(p)
	wbr.lsn += XLogRecPtr(n)
	return n, err
}

func (wbr *WalBufReader) Cur() XLogRecPtr {
	return wbr.lsn
}

func (wbr *WalBufReader) Size() int32 {
	return int32(wbr.Reader.Size())
}

func (wbr *WalBufReader) Len() int32 {
	return int32(wbr.Reader.Buffered())
}

func (wbr *WalBufReader) Discard(n int32) (int32, error) {
	cnt, err := wbr.Reader.Discard(int(n))
	wbr.lsn += XLogRecPtr(cnt)
	return int32(cnt), err
}
