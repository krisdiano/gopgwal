package wal

import (
	"bytes"
	"errors"
	"fmt"
	"os"
)

// XLogReader need a startpoint which is a beginning of page or a valid XLogRecPtr
type XLogReader struct {
	alignment uint8

	segmentSize uint32
	blockSize   uint32

	read   uint32
	cur    XLogRecPtr
	reader *BufFile
}

func NewXLogReader(path string, align uint8) (*XLogReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	hdr, err := ReadXLogLongPageHeader(f)
	if err != nil {
		return nil, err
	}
	if !IsValidXLogPageHeader(hdr) {
		return nil, fmt.Errorf("invalid segment file %s", path)
	}

	cur := hdr.Std.XlpPageAddr + XLogRecPtr(SizeofXLogLongPageHeaderData())
	// reader := bufio.NewReaderSize(f, int(hdr.XlpSegSize))
	reader := &BufFile{f}

	if hdr.Std.XlpInfo&XLP_FIRST_IS_CONTRECORD != 0 {
		if rDataLen := int(hdr.Std.XlpRemLen); rDataLen > 0 {
			_, err = reader.Discard(rDataLen)
			if err != nil {
				return nil, err
			}
			cur += XLogRecPtr(rDataLen)
		}
	}

	ret := &XLogReader{
		alignment:   align,
		segmentSize: hdr.XlpSegSize,
		blockSize:   hdr.XlpXLogBlcksz,
		cur:         cur,
		reader:      reader,
	}
	_, err = ret.align()
	if err != nil {
		return nil, err
	}
	ret.read = uint32(ret.cur - hdr.Std.XlpPageAddr)
	return ret, nil
}

func (r *XLogReader) isPageHeaderLSN() (page bool, seg bool) {
	return r.cur%XLogRecPtr(r.blockSize) == 0, r.cur%XLogRecPtr(r.segmentSize) == 0
}

func (r *XLogReader) remainBlkSize() uint32 {
	if a, _ := r.isPageHeaderLSN(); a {
		return r.blockSize
	}
	return r.blockSize - uint32(r.cur)%r.blockSize
}

func (r *XLogReader) remainSegSize() uint32 {
	if _, b := r.isPageHeaderLSN(); b {
		return r.segmentSize
	}
	return r.segmentSize - uint32(r.cur)%r.segmentSize
}

// currrent lsn must start be a record hdr or page header which has no cont record.
func (r *XLogReader) readN(size uint32) (lsn XLogRecPtr, _ []byte, err error) {
	if size == 0 {
		return 0, nil, errors.New("size must greater than 0")
	}
	if r.cur%XLogRecPtr(r.alignment) != 0 {
		return 0, nil, errors.New("lsn must be at page header or record header")
	}

	var (
		dis  int    = 0
		ret         = make([]byte, size)
		read uint32 = 0
	)
	for read < size {
		isPageHdr, isLongPageHdr := r.isPageHeaderLSN()
		switch {
		case isLongPageHdr:
			dis = int(SizeofXLogLongPageHeaderData())
		case isPageHdr:
			dis = int(SizeofXLogPageHeaderData())
		}

		if dis > 0 {
			_, err = r.reader.Discard(dis)
			if err != nil {
				return 0, nil, err
			}
			r.cur += XLogRecPtr(dis)
			r.read += uint32(dis)
			dis = 0
		}

		if read == 0 {
			lsn = r.cur
		}

		free := r.remainBlkSize()
		if (size - read) <= free {
			_, err := r.reader.Read(ret[read:])
			if err != nil {
				return 0, nil, err
			}

			r.cur += XLogRecPtr(size - read)
			r.read += uint32(size - read)
			return lsn, ret, nil
		}
		n, err := r.reader.Read(ret[read : read+free])
		if err != nil && uint32(n) != free {
			return 0, nil, err
		}
		read += free
		r.cur += XLogRecPtr(free)
		r.read += free
	}
	return 0, nil, errors.New("should not be here")
}

func (r *XLogReader) align() (XLogRecPtr, error) {
	offset := uint8(r.cur % XLogRecPtr(r.alignment))
	if offset == 0 {
		return r.cur, nil
	}

	step := r.alignment - offset
	_, err := r.reader.Discard(int(step))
	if err != nil {
		return 0, err
	}
	r.cur += XLogRecPtr(step)
	r.read += uint32(step)
	return r.cur, nil
}

func (r *XLogReader) ReadRecord() (*RawRecord, error) {
	_, err := r.align()
	if err != nil {
		return nil, err
	}

	lsn, rawhdr, err := r.readN(uint32(SizeofXLogRecord()))
	if err != nil {
		return nil, err
	}
	hdr, err := ReadXLogRecord(bytes.NewReader(rawhdr))
	if err != nil {
		return nil, err
	}

	if hdr.XlRmid == RM_XLOG_ID && hdr.XlInfo&(XLR_RMGR_INFO_MASK) == 0x40 {
		rDataLen := r.remainSegSize()
		_, err = r.reader.Discard(int(rDataLen))
		if err != nil {
			return nil, err
		}
		return &RawRecord{LSN: lsn, Hdr: hdr}, nil
	}

	_, rawdata, err := r.readN(hdr.XlTotlen - uint32(SizeofXLogRecord()))
	if err != nil {
		return nil, err
	}
	return &RawRecord{LSN: lsn, Hdr: hdr, data: rawdata}, nil
}
