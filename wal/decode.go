package wal

import (
	"bytes"
	"io"
)

type RawRecord struct {
	LSN  XLogRecPtr
	Hdr  *XLogRecord
	data []byte
}

func (rr *RawRecord) Decode() (*Record, error) {
	reader := bytes.NewReader(rr.data)
	ret := &Record{
		LSN: rr.LSN,
		Hdr: rr.Hdr,
	}
	total := rr.Hdr.XlTotlen - uint32(SizeofXLogRecord())
	queried := int64(0)
LOOP:
	for total > uint32(queried) {
		bid, err := ReadReferenceId(reader)
		if err != nil {
			return nil, err
		}

		switch {
		case bid <= XLR_MAX_BLOCK_ID:
			var (
				block   Block
				bheader *XLogRecordBlockHeader
				iheader *XLogRecordBlockImageHeader
			)
			bheader, err = ReadXLogRecordBlockHeader(reader, bid)
			if err != nil {
				return nil, err
			}
			block.Bheader = bheader
			queried += SizeofXLogRecordBlockHeader() + int64(bheader.DataLength)
			if bheader.HasImage() {
				iheader, err = ReadXLogRecordBlockImageHeader(reader)
				if err != nil {
					return nil, err
				}
				block.Iheader = iheader
				queried += SizeofXLogRecordBlockImageHeader() + int64(iheader.Length)
				if iheader.HasHole() && iheader.HasCompressed() {
					cheader, err := ReadXLogRecordBlockCompressHeader(reader)
					if err != nil {
						return nil, err
					}
					block.Cheader = cheader
					queried += SizeofXLogRecordBlockCompressHeader()
				}
			}
			if bheader.HasFileNode() {
				rfn, err := ReadRelFileNode(reader)
				if err != nil {
					return nil, err
				}
				block.RelFileNode = rfn
				queried += SizeofRelFileNode()
			}
			bn, err := ReadBlockNumber(reader)
			if err != nil {
				return nil, err
			}
			block.BlockNum = bn
			queried += SizeofBlockNumber()
			ret.Blocks = append(ret.Blocks, block)
		case bid == XLR_BLOCK_ID_ORIGIN:
			rod, err := ReadRepOriginDummy(reader, bid)
			if err != nil {
				return nil, err
			}
			ret.RepOriginId = rod.RepOriginId
			queried += SizeofRepOriginDummy()
		case bid == XLR_BLOCK_ID_DATA_SHORT:
			sheader, err := ReadXLogRecordDataHeaderShort(reader, bid)
			if err != nil {
				return nil, err
			}
			ret.MainData = make([]byte, sheader.DataLength)
			break LOOP
		case bid == XLR_BLOCK_ID_DATA_LONG:
			_, err = ReadXLogRecordDataHeaderLong(reader, bid)
			if err != nil {
				return nil, err
			}
			length, err := ReadMainDataLength(reader)
			if err != nil {
				return nil, err
			}
			ret.MainData = make([]byte, length)
			break LOOP
		}
	}

	var err error
	for i := range ret.Blocks {
		item := &ret.Blocks[i]
		if item.Iheader != nil && item.Iheader.Length > 0 {
			data := make([]byte, item.Iheader.Length)
			_, err = io.ReadFull(reader, data)
			if err != nil {
				return nil, err
			}
			item.PageData = data
		}
		if item.Bheader.HasData() {
			if item.Bheader.DataLength > 0 {
				data := make([]byte, item.Bheader.DataLength)
				_, err = io.ReadFull(reader, data)
				if err != nil {
					return nil, err
				}
				item.TupleData = data
			}
		}
	}

	if len(ret.MainData) > 0 {
		_, err = io.ReadFull(reader, ret.MainData)
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}

type Record struct {
	LSN         XLogRecPtr
	Hdr         *XLogRecord
	Blocks      []Block
	RepOriginId RepOriginId
	MainData    []byte
}

type Block struct {
	Bheader     *XLogRecordBlockHeader
	Iheader     *XLogRecordBlockImageHeader
	Cheader     *XLogRecordBlockCompressHeader
	PageData    []byte
	TupleData   []byte
	RelFileNode *RelFileNode
	BlockNum    BlockNumber
}
