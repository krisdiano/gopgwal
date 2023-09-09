package wal

import (
	"io"
	"unsafe"
)

const (
	XLR_INFO_MASK      = 0x0F
	XLR_RMGR_INFO_MASK = 0xF0

	/*
	 * If a WAL record modifies any relation files, in ways not covered by the
	 * usual block references, this flag is set. This is not used for anything
	 * by PostgreSQL itself, but it allows external tools that read WAL and keep
	 * track of modified blocks to recognize such special record types.
	 */
	XLR_SPECIAL_REL_UPDATE = 0x01

	/*
	 * Enforces consistency checks of replayed WAL at recovery. If enabled,
	 * each record will log a full-page write for each block modified by the
	 * record and will reuse it afterwards for consistency checks. The caller
	 * of XLogInsert can use this value if necessary, but if
	 * wal_consistency_checking is enabled for a rmgr this is set unconditionally.
	 */
	XLR_CHECK_CONSISTENCY = 0x02
)

const (
	XLR_MAX_BLOCK_ID uint8 = 32

	XLR_BLOCK_ID_DATA_SHORT   uint8 = 255
	XLR_BLOCK_ID_DATA_LONG    uint8 = 254
	XLR_BLOCK_ID_ORIGIN       uint8 = 253
	XLR_BLOCK_ID_TOPLEVEL_XID uint8 = 252
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

func SizeofXLogRecord() int64 {
	return 24
}

func ReadReferenceId(reader io.Reader) (uint8, error) {
	id := make([]byte, 1)
	_, err := io.ReadFull(reader, id)
	if err != nil {
		return 0, err
	}
	return uint8(id[0]), nil
}

const (
	BKPBLOCK_FORK_MASK = 0x0F
	BKPBLOCK_FLAG_MASK = 0xF0
	BKPBLOCK_HAS_IMAGE = 0x10 /* block data is an XLogRecordBlockImage */
	BKPBLOCK_HAS_DATA  = 0x20
	BKPBLOCK_WILL_INIT = 0x40 /* redo will re-init the page */
	BKPBLOCK_SAME_REL  = 0x80
)

type XLogRecordBlockHeader struct {
	Id         uint8  /* block reference ID */
	ForkFlags  uint8  /* fork within the relation, and flags */
	DataLength uint16 /* number of payload bytes (not including page
	* image) */

	/* If BKPBLOCK_HAS_IMAGE, an XLogRecordBlockImageHeader struct follows */
	/* If BKPBLOCK_SAME_REL is not set, a RelFileNode follows */
	/* BlockNumber follows */
}

func SizeofXLogRecordBlockHeader() int64 {
	return 4
}

func (h *XLogRecordBlockHeader) HasData() bool {
	return h.ForkFlags&BKPBLOCK_FLAG_MASK&BKPBLOCK_HAS_DATA == BKPBLOCK_HAS_DATA
}

func (h *XLogRecordBlockHeader) HasImage() bool {
	return h.ForkFlags&BKPBLOCK_FLAG_MASK&BKPBLOCK_HAS_IMAGE == BKPBLOCK_HAS_IMAGE
}

func (h *XLogRecordBlockHeader) HasFileNode() bool {
	return h.ForkFlags&BKPBLOCK_FLAG_MASK&BKPBLOCK_SAME_REL == 0
}

func ReadXLogRecordBlockHeader(reader io.Reader, id uint8) (*XLogRecordBlockHeader, error) {
	var header XLogRecordBlockHeader
	buf := make([]byte, SizeofXLogRecordBlockHeader())
	buf[0] = id
	_, err := io.ReadFull(reader, buf[1:])
	if err != nil {
		return nil, err
	}
	ptr := (*XLogRecordBlockHeader)(unsafe.Pointer(&buf[0]))
	header = *ptr
	return &header, nil
}

const (
	/* Information stored in bimg_info */
	BKPIMAGE_HAS_HOLE = 0x01 /* page image has "hole" */
	// BKPIMAGE_APPLY    = 0x02 /* page image should be restored
	//  * during replay */
	// /* compression methods supported */
	// BKPIMAGE_COMPRESS_PGLZ = 0x04
	// BKPIMAGE_COMPRESS_LZ4  = 0x08
	// BKPIMAGE_COMPRESS_ZSTD = 0x10

	BKPIMAGE_IS_COMPRESSED = 0x02
)

type XLogRecordBlockImageHeader struct {
	Length     uint16 /* number of page image bytes */
	HoleOffset uint16 /* number of bytes before "hole" */
	BimgInfo   uint8  /* flag bits, see below */

	/*
	 * If BKPIMAGE_HAS_HOLE and BKPIMAGE_COMPRESSED(), an
	 * XLogRecordBlockCompressHeader struct follows.
	 */
}

func SizeofXLogRecordBlockImageHeader() int64 {
	return 5
}

func (h *XLogRecordBlockImageHeader) HasHole() bool {
	return h.BimgInfo&BKPIMAGE_HAS_HOLE == BKPIMAGE_HAS_HOLE
}

// func (h *XLogRecordBlockImageHeader) HasCompressed() bool {
// 	return (h.BimgInfo & (BKPIMAGE_COMPRESS_PGLZ | BKPIMAGE_COMPRESS_LZ4 | BKPIMAGE_COMPRESS_ZSTD)) != 0
// }

func (h *XLogRecordBlockImageHeader) HasCompressed() bool {
	return h.BimgInfo&BKPIMAGE_IS_COMPRESSED == BKPIMAGE_IS_COMPRESSED
}

func ReadXLogRecordBlockImageHeader(reader io.Reader) (*XLogRecordBlockImageHeader, error) {
	var header XLogRecordBlockImageHeader
	buf := make([]byte, SizeofXLogRecordBlockImageHeader())
	_, err := io.ReadFull(reader, buf)
	if err != nil {
		return nil, err
	}
	ptr := (*XLogRecordBlockImageHeader)(unsafe.Pointer(&buf[0]))
	header = *ptr
	return &header, nil
}

type XLogRecordBlockCompressHeader struct {
	HoleLength uint16 /* number of bytes in "hole" */
}

func SizeofXLogRecordBlockCompressHeader() int64 {
	return 2
}

func ReadXLogRecordBlockCompressHeader(reader io.Reader) (*XLogRecordBlockCompressHeader, error) {
	var header XLogRecordBlockCompressHeader
	buf := make([]byte, SizeofXLogRecordBlockCompressHeader())
	_, err := io.ReadFull(reader, buf)
	if err != nil {
		return nil, err
	}
	ptr := (*XLogRecordBlockCompressHeader)(unsafe.Pointer(&buf[0]))
	header = *ptr
	return &header, nil
}

type Oid uint32
type RelFileNode struct {
	SpcNode Oid /* tablespace */
	DbNode  Oid /* database */
	RelNode Oid /* relation */
}

func SizeofRelFileNode() int64 {
	return 12
}

func ReadRelFileNode(reader io.Reader) (*RelFileNode, error) {
	var rfn RelFileNode
	buf := make([]byte, SizeofRelFileNode())
	_, err := io.ReadFull(reader, buf)
	if err != nil {
		return nil, err
	}
	ptr := (*RelFileNode)(unsafe.Pointer(&buf[0]))
	rfn = *ptr
	return &rfn, nil
}

type BlockNumber uint32

func SizeofBlockNumber() int64 {
	return 4
}

func ReadBlockNumber(reader io.Reader) (BlockNumber, error) {
	var num BlockNumber
	buf := make([]byte, SizeofBlockNumber())
	_, err := io.ReadFull(reader, buf)
	if err != nil {
		return 0, err
	}
	ptr := (*BlockNumber)(unsafe.Pointer(&buf[0]))
	num = *ptr
	return num, nil
}

type RepOriginId uint16
type RepOriginDummy struct {
	Id          uint8 /*XLR_BLOCK_ID_ORIGIN*/
	RepOriginId RepOriginId
}

func SizeofRepOriginDummy() int64 {
	return 3
}

func ReadRepOriginDummy(reader io.Reader, id uint8) (*RepOriginDummy, error) {
	var header RepOriginDummy
	buf := make([]byte, SizeofRepOriginDummy())
	buf[0] = id
	_, err := io.ReadFull(reader, buf[1:])
	if err != nil {
		return nil, err
	}
	ptr := (*RepOriginDummy)(unsafe.Pointer(&buf[0]))
	header = *ptr
	return &header, nil
}

type XLogRecordDataHeaderShort struct {
	Id         uint8 /* XLR_BLOCK_ID_DATA_SHORT */
	DataLength uint8 /* number of payload bytes */
}

func SizeofXLogRecordDataHeaderShort() int64 {
	return 2
}

func ReadXLogRecordDataHeaderShort(reader io.Reader, id uint8) (*XLogRecordDataHeaderShort, error) {
	var header XLogRecordDataHeaderShort
	buf := make([]byte, SizeofXLogRecordDataHeaderShort())
	buf[0] = id
	_, err := io.ReadFull(reader, buf[1:])
	if err != nil {
		return nil, err
	}
	ptr := (*XLogRecordDataHeaderShort)(unsafe.Pointer(&buf[0]))
	header = *ptr
	return &header, nil
}

type XLogRecordDataHeaderLong struct {
	Id uint8 /* XLR_BLOCK_ID_DATA_LONG */
	/* followed by uint32 data_length, unaligned */
}

func SizeofXLogRecordDataHeaderLong() int64 {
	return 1
}

func ReadXLogRecordDataHeaderLong(reader io.Reader, id uint8) (*XLogRecordDataHeaderLong, error) {
	return &XLogRecordDataHeaderLong{id}, nil
}

// ReadMainDataLength should only be called when id equals XLR_BLOCK_ID_DATA_LONG
func ReadMainDataLength(reader io.Reader) (uint32, error) {
	var length uint32
	buf := make([]byte, 4)
	_, err := io.ReadFull(reader, buf)
	if err != nil {
		return 0, err
	}
	ptr := (*uint32)(unsafe.Pointer(&buf[0]))
	length = *ptr
	return length, nil
}

func ReadXLogRecord(reader io.Reader) (*XLogRecord, error) {
	var (
		record  XLogRecord
		content = make([]byte, unsafe.Sizeof(record))
	)

	_, err := io.ReadFull(reader, content)
	if err != nil {
		return nil, err
	}
	ptr := (*XLogRecord)(unsafe.Pointer(&content[0]))
	record = *ptr
	return &record, nil
}
