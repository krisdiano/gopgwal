package wal

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
)

// XLogReader need a startpoint which is a beginning of page or a valid XLogRecPtr
type XLogReader struct {
	alignment   uint8
	segmentSize uint32
	blockSize   uint32
	reader      *WalBufReader
}

func NewXLogReader(align uint8, segmentSize uint32, blockSize uint32, lsn XLogRecPtr, reader io.Reader) (*XLogReader, error) {
	if align < 1 {
		return nil, fmt.Errorf("invalid alignment")
	}
	if reader == nil {
		return nil, fmt.Errorf("nil reader")
	}
	if blockSize == 0 || segmentSize == 0 || blockSize&1024 != 0 || segmentSize%blockSize != 0 {
		return nil, fmt.Errorf("invalid size")
	}
	if blockSize > segmentSize {
		return nil, fmt.Errorf("block size greater than segment size")
	}
	return &XLogReader{
		alignment:   align,
		segmentSize: segmentSize,
		blockSize:   blockSize,
		reader:      NewWalBufReader(lsn, bufio.NewReaderSize(reader, 1024*1024*64)),
	}, nil
}

func (r *XLogReader) LSN() XLogRecPtr {
	return r.reader.Cur()
}

func (r *XLogReader) FindNextRecordWithPageLSN() (XLogRecPtr, error) {
	header, err := r.readPageHeader(r.LSN())
	if err != nil {
		return 0, err
	}

	length := r.remainDataLen(header)
	if length == 0 {
		return r.LSN(), nil
	}
	size := r.remainBlockSize()
	if length < size {
		cur, isNewBlock, err := r.alignN(int32(length))
		if err != nil {
			return 0, err
		}
		if isNewBlock {
			return r.FindNextRecordWithPageLSN()
		}
		if size = r.remainBlockSize(); size >= uint32(SizeofXLogRecord()) {
			return cur, nil
		}
		cur, _, err = r.alignN(int32(size))
		if err != nil {
			return 0, err
		}
		return r.FindNextRecordWithPageLSN()
	}
	_, _, err = r.alignN(int32(size))
	if err != nil {
		return 0, err
	}
	return r.FindNextRecordWithPageLSN()
}

func (r *XLogReader) FindNextRecordWithTailLSN() (XLogRecPtr, error) {
	cur, isNewBlock, err := r.align()
	if err != nil {
		return 0, err
	}
	if isNewBlock {
		return r.FindNextRecordWithPageLSN()
	}
	remain := r.remainBlockSize()
	if remain >= uint32(SizeofXLogRecord()) {
		return cur, nil
	}
	cur, _, err = r.alignN(int32(remain))
	if err != nil {
		return 0, err
	}
	return r.FindNextRecordWithPageLSN()
}

func (r *XLogReader) readPageHeader(lsn XLogRecPtr) (XLogPageHeader, error) {
	isLong := r.isLongPageHeaderLSN(lsn)
	isStd := r.isPageHeaderLSN(lsn)
	if !isLong && !isStd {
		return nil, fmt.Errorf("expected a page lsn, got %s", lsn)
	}

	if isLong {
		header, err := ReadXLogLongPageHeader(r.reader)
		if err != nil {
			return nil, err
		}
		return &header.Std, nil
	}
	return ReadXLogPageHeader(r.reader)
}

func (r *XLogReader) align() (XLogRecPtr, bool, error) {
	b := r.LSN() % XLogRecPtr(r.alignment)
	if b != 0 {
		delta := int32(r.alignment) - int32(b)
		_, err := r.reader.Discard(delta)
		if err != nil {
			return 0, false, err
		}
	}
	cur := r.LSN()
	return cur, r.isLongPageHeaderLSN(cur) || r.isPageHeaderLSN(cur), nil
}

func (r *XLogReader) alignN(n int32) (XLogRecPtr, bool, error) {
	a, b := n/int32(r.alignment), n%int32(r.alignment)
	if b != 0 {
		n = a + int32(r.alignment)
	}
	_, err := r.reader.Discard(n)
	if err != nil {
		return 0, false, err
	}
	cur := r.LSN()
	return cur, r.isLongPageHeaderLSN(cur) || r.isPageHeaderLSN(cur), nil
}

func (r *XLogReader) remainBlockSize() uint32 {
	return r.blockSize - uint32(r.LSN())%r.blockSize
}

func (r *XLogReader) isLongPageHeaderLSN(lsn XLogRecPtr) bool {
	return lsn%XLogRecPtr(r.segmentSize) == 0
}

func (r *XLogReader) isPageHeaderLSN(lsn XLogRecPtr) bool {
	return lsn%XLogRecPtr(r.segmentSize) != 0 && lsn%XLogRecPtr(r.blockSize) == 0
}

func (r *XLogReader) remainDataLen(header XLogPageHeader) uint32 {
	hasRemainData := header.XlpInfo&XLP_ALL_FLAGS&XLP_FIRST_IS_CONTRECORD == XLP_FIRST_IS_CONTRECORD
	if !hasRemainData {
		return 0
	}
	return header.XlpRemLen
}

func (r *XLogReader) ReadRecord() (*RawRecord, error) {
	lsn := r.LSN()
	hdr, err := ReadXLogRecord(r.reader)
	if err != nil {
		return nil, err
	}

	cur := r.LSN()
	if r.isLongPageHeaderLSN(cur) || r.isPageHeaderLSN(cur) {
		_, err = r.readPageHeader(cur)
		if err != nil {
			return nil, err
		}
	}

	var (
		data   bytes.Buffer
		total  = hdr.XlTotlen - uint32(SizeofXLogRecord())
		remain = r.remainBlockSize()
	)
	if remain >= total {
		_, err = io.CopyN(&data, r.reader, int64(total))
		if err != nil {
			return nil, err
		}
	} else {
		if remain > 0 {
			_, err = io.CopyN(&data, r.reader, int64(remain))
			if err != nil {
				return nil, err
			}
		}

		for data.Len() < int(total) {
			header, err := r.readPageHeader(r.LSN())
			if err != nil {
				return nil, err
			}
			remain = r.remainDataLen(header)
			_, err = io.CopyN(&data, r.reader, int64(remain))
			if err != nil {
				return nil, err
			}
		}
	}

	return &RawRecord{LSN: lsn, Hdr: hdr, data: data.Bytes()}, nil
}
