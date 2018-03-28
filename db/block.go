// block.go
//
// Copyright (c) 2018 Yanke Guo <guoyk.cn@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import (
	"encoding/binary"
	"io"
	"os"
)

// BlockScanFunc callback function for scanning
type BlockScanFunc func(BlockEntry, *bool)

// BlockEntry a entry in index file, 40 byte
type BlockEntry struct {
	Epoch   int64  // unix timestamp in milliseconds
	Payload []byte // payload is the content of log
}

// BlockFile index file
type BlockFile struct {
	*os.File
}

// OpenBlockFile create a new index from file
func OpenBlockFile(filename string) (i *BlockFile, err error) {
	// open file
	var file *os.File
	if file, err = os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0640); err != nil {
		return
	}
	// create instance
	i = &BlockFile{file}
	return
}

// EntryAt returns a BlockEntry at offset
func (i *BlockFile) EntryAt(offset int64) (e BlockEntry, err error) {
	buf := make([]byte, 16, 16)
	if _, err = i.ReadAt(buf, offset); err != nil {
		return
	}
	e.Epoch = int64(binary.BigEndian.Uint64(buf))
	len := int64(binary.BigEndian.Uint64(buf[8:]))
	e.Payload = make([]byte, len, len)
	if _, err = i.ReadAt(e.Payload, offset+16); err != nil {
		return
	}
	return
}

// ScanEntries scan the whole index file
func (i *BlockFile) ScanEntries(fn BlockScanFunc) (err error) {
	var stop bool
	var e BlockEntry
	var c int64
	for {
		if e, err = i.EntryAt(c); err != nil {
			if err == io.EOF {
				err = nil
			}
			return
		}
		fn(e, &stop)
		if stop {
			break
		}
		c += 16 + int64(len(e.Payload))
	}
	return
}

// WriteEntry insert a entry
func (i *BlockFile) WriteEntry(e BlockEntry) (err error) {
	buf := make([]byte, 16, 16)
	encodeInt64(buf, e.Epoch)
	encodeInt64(buf[8:], int64(len(e.Payload)))
	if _, err = i.Write(buf); err != nil {
		return
	}
	if _, err = i.Write(e.Payload); err != nil {
		return
	}
	return
}
