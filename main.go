package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const REDIS_ADDR string = ":16379"
const REDIS_PASS string = "111111"

func case1() {
	//смесь короткоживущих и длинноживущие в одной базе
	rdb := redis.NewClient(&redis.Options{
		Addr:     REDIS_ADDR,
		Password: REDIS_PASS,
	})
	keyCount, err := genKeys("D", 30_000_000, const24Hour, *rdb)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("total keys(D): ", keyCount)
	}
	keyCountM, errM := genKeys("M", 2_000_000, rndMinute, *rdb)
	if errM != nil {
		panic(errM)
	} else {
		fmt.Println("total keys(M): ", keyCountM)
	}
	time.Sleep(60 * time.Second)
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		if info, err := rdb.Info(ctx, "keyspace").Result(); err != nil {
			panic(err)
		} else {
			fmt.Println("ts:", time.Now().Format("03:04:05 PM"), "info: ", info)
		}
		time.Sleep(10 * time.Second)
	}
}

func case2() {
	//короткоживущие в отдельной базе
	rdb0 := redis.NewClient(&redis.Options{
		Addr:     REDIS_ADDR,
		Password: REDIS_PASS,
		DB:       0,
	})
	keyCountD, errD := genKeys("D", 30_000_000, const24Hour, *rdb0)
	if errD != nil {
		panic(errD)
	} else {
		fmt.Println("total keys(D): ", keyCountD)
	}
	rdb1 := redis.NewClient(&redis.Options{
		Addr:     REDIS_ADDR,
		Password: REDIS_PASS,
		DB:       1,
	})
	keyCountM, errM := genKeys("M", 2_000_000, rndMinute, *rdb1)
	if errM != nil {
		panic(errM)
	} else {
		fmt.Println("total keys(M): ", keyCountM)
	}
	time.Sleep(40 * time.Second)
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		if info, err := rdb1.Info(ctx, "keyspace").Result(); err != nil {
			panic(err)
		} else {
			fmt.Println("ts:", time.Now().Format("03:04:05 PM"), "info: ", info)
		}
		time.Sleep(10 * time.Second)
	}
}
func case3() {
	//короткиживущие и длинноживущие в hashset в listpack
	rdb := redis.NewClient(&redis.Options{
		Addr:     REDIS_ADDR,
		Password: REDIS_PASS,
		DB:       0,
	})
	minuteFraction := float32(0.067)
	const HASH_FIELDS_SIZE = 500
	if keyCount, err := pipelineHt(32_000_000, *rdb, 0.067, HASH_FIELDS_SIZE, 7); err != nil {
		panic(err)
	} else {
		fmt.Println("ts:", time.Now().Format("01/02/06 03:04:05 PM"), "total keys(HS): ", keyCount)
	}
	if avgLen, err := avgHsetLen(HASH_FIELDS_SIZE, *rdb); err != nil {
		panic(err)
	} else {
		fmt.Println("ts:", time.Now().Format("01/02/06 03:04:05 PM"), "avgLen(before): ", avgLen)
	}
	time.Sleep(60 * time.Second)
	//цикл для оценки хода экспирации
	var threshold float32 = float32(HASH_FIELDS_SIZE) * minuteFraction
	var curDiff float32 = 0.0
	for curDiff < threshold {
		if avgLen, err := avgHsetLen(HASH_FIELDS_SIZE, *rdb); err != nil {
			panic(err)
		} else {
			fmt.Println("ts:", time.Now().Format("01/02/06 03:04:05 PM"), "avgLen(after): ", avgLen)
			curDiff = HASH_FIELDS_SIZE - avgLen
		}
		time.Sleep(10 * time.Second)
	}
}
func case4() {
	//короткиживущие и длинноживущие в hashset в listpack
	rdb := redis.NewClient(&redis.Options{
		Addr:     REDIS_ADDR,
		Password: REDIS_PASS,
		DB:       0,
	})
	//32_000_000
	minuteFraction := float32(0.067)
	const HASH_FIELDS_SIZE = 1000
	if keyCount, err := pipelineHt(32_000_000, *rdb, 0.067, HASH_FIELDS_SIZE, 2); err != nil {
		panic(err)
	} else {
		fmt.Println("ts:", time.Now().Format("03:04:05 PM"), "total keys(HS): ", keyCount)
	}
	if avgLen, err := avgHsetLen(HASH_FIELDS_SIZE, *rdb); err != nil {
		panic(err)
	} else {
		fmt.Println("ts:", time.Now().Format("03:04:05 PM"), "avgLen(before): ", avgLen)
	}
	time.Sleep(60 * time.Second)
	//цикл для оценки хода экспирации
	var threshold float32 = float32(HASH_FIELDS_SIZE) * minuteFraction
	var curDiff float32 = 0.0
	for curDiff < threshold {
		if avgLen, err := avgHsetLen(HASH_FIELDS_SIZE, *rdb); err != nil {
			panic(err)
		} else {
			fmt.Println("ts:", time.Now().Format("03:04:05 PM"), "avgLen(after): ", avgLen)
			curDiff = HASH_FIELDS_SIZE - avgLen
		}
		time.Sleep(10 * time.Second)
	}
}

func case5() {
	//экспирация большого количества сущностей "в моменте"
	rdb0 := redis.NewClient(&redis.Options{
		Addr:     REDIS_ADDR,
		Password: REDIS_PASS,
		DB:       0,
	})
	keyCountD, errD := genKeys("D", 30_000_000, rndMinute, *rdb0)
	if errD != nil {
		panic(errD)
	} else {
		fmt.Println("total keys(D): ", keyCountD)
	}
}

func main() {

	fmt.Println("ts:", time.Now().Format("03:04:05 PM"), ">>")
	start := time.Now()
	case5()
	end := time.Now()
	fmt.Println("ts:", time.Now().Format("03:04:05 PM"), "<<")
	fmt.Println("started:", start, "; ended:", end)
}
