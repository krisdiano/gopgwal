package wal

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"unsafe"
)

const (
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

type TimeLineID uint32
type XLogRecPtr uint64

func (lsn XLogRecPtr) String() string {
	high := lsn & 0xffffffff00000000 >> 32
	tmp := lsn & 0x00000000ffffffff
	logSeq := tmp & 0xff000000 >> 24
	offset := tmp & 0x00ffffff
	return fmt.Sprintf("%x/%02x%06x", uint32(high), uint32(logSeq), uint32(offset))
}

type XLogPageHeaderData struct {
	XlpMagic    uint16
	XlpInfo     uint16
	XlpTli      TimeLineID
	XlpPagedddr XLogRecPtr
	XlpRemLen   uint32
}

func SizeofXLogPageHeaderData() int64 {
	return 24
}

func (x *XLogPageHeaderData) String() string {
	var s strings.Builder
	s.WriteString("magic")
	s.WriteByte('=')
	s.WriteString(strconv.FormatUint(uint64(x.XlpMagic), 16))
	s.WriteByte('\n')
	s.WriteString("info")
	s.WriteByte('=')
	s.WriteString(strconv.FormatUint(uint64(x.XlpInfo), 16))
	s.WriteByte('\n')
	s.WriteString("tli")
	s.WriteByte('=')
	s.WriteString(strconv.FormatUint(uint64(x.XlpTli), 16))
	s.WriteByte('\n')
	s.WriteString("pageaddr")
	s.WriteByte('=')
	s.WriteString(strconv.FormatUint(uint64(x.XlpPagedddr), 16))
	s.WriteByte('\n')
	s.WriteString("rem_len")
	s.WriteByte('=')
	s.WriteString(strconv.FormatUint(uint64(x.XlpRemLen), 16))
	return s.String()
}

type XLogPageHeader = *XLogPageHeaderData

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

type XLogLongPageHeaderData struct {
	Std           XLogPageHeaderData
	XlpSysid      uint64
	XlpSegSize    uint32
	XlpXLogBlcksz uint32
}

func SizeofXLogLongPageHeaderData() int64 {
	return 40
}

func (x *XLogLongPageHeaderData) String() string {
	var s strings.Builder
	s.WriteString(x.Std.String())
	s.WriteByte('\n')
	s.WriteString("sysid")
	s.WriteByte('=')
	s.WriteString(strconv.FormatUint(x.XlpSysid, 16))
	s.WriteByte('\n')
	s.WriteString("seg_size")
	s.WriteByte('=')
	s.WriteString(strconv.FormatUint(uint64(x.XlpSegSize), 16))
	s.WriteByte('\n')
	s.WriteString("tli")
	s.WriteByte('=')
	s.WriteString(strconv.FormatUint(uint64(x.XlpXLogBlcksz), 16))
	return s.String()
}

type XLogLongPageHeader = *XLogLongPageHeaderData

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
