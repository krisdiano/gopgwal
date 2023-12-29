package wal

import (
	"io"
	"unsafe"
)

const (
	XLOG_PAGE_MAGIC = 0xD101

	/* When record crosses page boundary, set this flag in new page's header */
	XLP_FIRST_IS_CONTRECORD = 0x0001
	/* This flag indicates a "long" page header */
	XLP_LONG_HEADER = 0x0002
	/* This flag indicates backup blocks starting in this page are optional */
	XLP_BKP_REMOVABLE = 0x0004
	/* Replaces a missing contrecord; see CreateOverwriteContrecordRecord */
	XLP_FIRST_IS_OVERWRITE_CONTRECORD = 0x0008
	/* All defined flag bits in xlp_info (used for validity checking of header) */
	XLP_ALL_FLAGS = 0x000F
)

type XLogPageHeaderData struct {
	XlpMagic    uint16
	XlpInfo     uint16
	XlpTli      TimeLineID
	XlpPageAddr XLogRecPtr
	XlpRemLen   uint32
}

type XLogPageHeader = *XLogPageHeaderData

func SizeofXLogPageHeaderData() int64 {
	return 24
}

func ReadXLogPageHeader(reader io.Reader) (XLogPageHeader, error) {
	var (
		header  XLogPageHeaderData
		content = make([]byte, SizeofXLogPageHeaderData())
	)

	_, err := io.ReadFull(reader, content)
	if err != nil {
		return nil, err
	}
	ptr := (XLogPageHeader)(unsafe.Pointer(&content[0]))
	header = *ptr
	return &header, nil
}

func IsValidXLogPageHeader(ptr XLogLongPageHeader) bool {
	return ptr.Std.XlpMagic == XLOG_PAGE_MAGIC
}

type XLogLongPageHeaderData struct {
	Std           XLogPageHeaderData
	XlpSysid      uint64
	XlpSegSize    uint32
	XlpXLogBlcksz uint32
}

type XLogLongPageHeader = *XLogLongPageHeaderData

func SizeofXLogLongPageHeaderData() int64 {
	return 40
}

func ReadXLogLongPageHeader(reader io.Reader) (XLogLongPageHeader, error) {
	var (
		longHeader XLogLongPageHeaderData
		content    = make([]byte, SizeofXLogLongPageHeaderData())
	)

	_, err := io.ReadFull(reader, content)
	if err != nil {
		return nil, err
	}
	ptr := (XLogLongPageHeader)(unsafe.Pointer(&content[0]))
	longHeader = *ptr
	return &longHeader, nil
}
