package Utils

import (
	"hash/crc32"
)



func Key2shard(key string,shardNum int) int {
	return int(crc32.ChecksumIEEE([]byte(key))) % shardNum
}

func Contains(s []int,sid int) bool{
	for _,entry := range s {
		if entry == sid {
			return true
		}
	}
	return false
}