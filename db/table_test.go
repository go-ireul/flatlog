// table_test.go
//
// Copyright (c) 2018 Yanke Guo <guoyk.cn@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import (
	"fmt"
	"log"
	mrand "math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTable(t *testing.T) {
	b, err := OpenTable(filepath.Join(os.TempDir(), fmt.Sprintf("%016x", randomUint64())))
	if err != nil {
		t.Error(err)
	}
	log.Println(b.dir)
	t0 := time.Now().Unix() * 1000
	for i := 100000; i > 0; i-- {
		pl := make([]byte, randomCount())
		mrand.Read(pl)
		b.Append(t0-int64(i*2000), string(pl))
	}
	b.Close()
}
