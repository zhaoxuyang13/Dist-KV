package Utils

import (
	"hash/crc32"
	"log"
	"math/rand"
	"time"
)



func Key2shard(key string,shardNum int) int {
	return int(crc32.ChecksumIEEE([]byte(key))) % shardNum
}

func ContainsInt(s []int,sid int) bool{
	for _,entry := range s {
		if entry == sid {
			return true
		}
	}
	return false
}

func Delete(s []int, sid int) []int{
	for index,entry := range s {
		if entry == sid {
			return append(s[:index],s[index+1:]...)
		}
	}
	return s
}
func PrintErrTrace(){
	if r := recover(); r != nil {
		log.Printf("err recovered: %+v\n",r)
	}
}


const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seededRand *rand.Rand = rand.New(
	rand.NewSource(time.Now().UnixNano()))

func StringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func RandString(length int) string {
	return StringWithCharset(length, charset)
}
func RandInt(maximum int) int {
	return seededRand.Intn(maximum)
}