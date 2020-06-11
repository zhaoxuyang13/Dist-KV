package Utils

import (
	"hash/crc32"
)



func Key2shard(key string,shardNum int) int {
	return int(crc32.ChecksumIEEE([]byte(key))) % shardNum
}
