// utils.go
//
// Copyright (c) 2018 Yanke Guo <guoyk.cn@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import (
	"encoding/binary"
)

func decodeInt64(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf))
}

func encodeInt64(buf []byte, v int64) {
	binary.BigEndian.PutUint64(buf, uint64(v))
}
