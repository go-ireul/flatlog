// block_test.go
//
// Copyright (c) 2018 Yanke Guo <guoyk.cn@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import (
	"bytes"
	srand "crypto/rand"
	"encoding/binary"
	"fmt"
	mrand "math/rand"
	"os"
	"path/filepath"
	"testing"
)

func tempFile() string {
	dir := os.TempDir()
	os.MkdirAll(dir, 0750)
	return filepath.Join(dir, fmt.Sprintf("blk-%02x", randomUint64()))
}

func randomCount() int {
	return int(mrand.Float32()*1000) + 100
}

func randomUint64() uint64 {
	buf := make([]byte, 8, 8)
	srand.Read(buf)
	return binary.BigEndian.Uint64(buf)
}

func randomInt64() int64 {
	i := int64(randomUint64())
	if i < 0 {
		return -i
	}
	return i
}

func TestBlockFile(t *testing.T) {
	var err error
	var i *BlockFile
	// create file
	file := tempFile()
	if i, err = OpenBlockFile(file); err != nil {
		t.Fatal(err)
	}
	// create source entries
	entries := []BlockEntry{}
	count := randomCount()
	for j := 0; j < count; j++ {
		pl := make([]byte, randomCount())
		srand.Read(pl)
		entries = append(entries, BlockEntry{
			Epoch:   randomInt64(),
			Payload: pl,
		})
	}
	// insert entries
	for _, e := range entries {
		if err = i.WriteEntry(e); err != nil {
			t.Errorf("error: %v", err)
		}
	}
	i.Sync()
	// returns
	rets := []BlockEntry{}
	i.ScanEntries(func(e BlockEntry, stop *bool) {
		rets = append(rets, e)
	})
	// check entries
	for i, e0 := range entries {
		e1 := rets[i]
		if e0.Epoch != e1.Epoch || !bytes.Equal(e0.Payload, e1.Payload) {
			t.Errorf("entry not equal: %d", e1.Epoch)
		}
	}
	// close file
	i.Close()
	// remove file
	os.Remove(file)
}
