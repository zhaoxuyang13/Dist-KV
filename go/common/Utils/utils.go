package Utils

import (
	"hash/crc32"
	"log"
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
/*
func Delete(slice interface{}, element interface{}) ([]interface{},error){
	sli := reflect.ValueOf(slice)
	ele := reflect.ValueOf(element)
	if sli.Kind() != reflect.Slice {
		return nil, errors.New("first argument should be a slice")
	}
	len := sli.Len()
	if len < 0 {
		return nil, errors.New("element not exist")
	}
	if sli.Index(0).Kind() != ele.Kind() {
		return nil, errors.New("slice and element type not match")
	}
	for i := 0; i < len; i ++ {
		if sli.Index(i) == ele {
			out := make()

			return out, nil
		}
	}

	return nil, errors.New("element not exist")
}*/