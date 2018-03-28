// table.go
// Copyright (c) 2018 Yanke Guo <guoyk.cn@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

func epochToDateStr(t int64) string {
	y, m, d := time.Unix(t/1000, 0).Date()
	return fmt.Sprintf("%04d-%02d-%02d", y, m, d)
}

// Table table represents a time series table
type Table struct {
	dir  string
	blks map[string]*BlockFile
	mtx  *sync.Mutex
}

// OpenTable create a new table instance from target dir
func OpenTable(dir string) (t *Table, err error) {
	if err = os.MkdirAll(dir, 0750); err != nil {
		return
	}
	t = &Table{
		dir:  dir,
		blks: map[string]*BlockFile{},
		mtx:  &sync.Mutex{},
	}
	return
}

// Close close all index files and all block files
func (b *Table) Close() {
	b.mtx.Lock()
	for _, blk := range b.blks {
		blk.Close()
	}
	b.blks = map[string]*BlockFile{}
	b.mtx.Unlock()
}

func (b *Table) open(date string) (blk *BlockFile, err error) {
	blk = b.blks[date]
	if blk == nil {
		b.mtx.Lock()
		blk = b.blks[date]
		if blk == nil {
			blk, err = OpenBlockFile(filepath.Join(b.dir, date+".flog"))
		}
		if blk != nil {
			b.blks[date] = blk
		}
		b.mtx.Unlock()
	}
	return
}

// Append append a log line to table
func (b *Table) Append(t int64, s string) (err error) {
	date := epochToDateStr(t)
	var blk *BlockFile
	if blk, err = b.open(date); err != nil {
		return
	}
	if err = blk.WriteEntry(BlockEntry{
		Epoch:   t,
		Payload: []byte(s),
	}); err != nil {
		return
	}
	return
}
