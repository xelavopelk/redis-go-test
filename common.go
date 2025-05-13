package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/redis/go-redis/v9"
)

const BATCH_SIZE int = 100_000

type expirator func() time.Duration

func const24Hour() time.Duration {
	return 24 * time.Hour
}

func rndMinute() time.Duration {
	return time.Minute + time.Duration(rand.IntN(60))*time.Second
}

func runPipeline(batch map[string]string, fn expirator, rdb redis.Client, ctx context.Context) (int, error) {
	pipe := rdb.Pipeline()
	pipe.MSet(ctx, batch)
	for key, _ := range batch {
		pipe.Expire(ctx, key, fn())
	}
	if _, err := pipe.Exec(ctx); err == nil {
		return len(batch), err
	} else {
		return 0, err
	}
}

func genKeys(preffix string, size int, fn expirator, rdb redis.Client) (int, error) {
	ctx := context.Background()
	todo := size
	counter := 0
	for todo > BATCH_SIZE {
		batch := genFakeBatch(preffix, BATCH_SIZE)
		if cow, err := runPipeline(batch, fn, rdb, ctx); err == nil {
			counter += cow
			todo -= cow
		} else {
			return counter, err
		}
		fmt.Println("loaded: ", counter)
	}
	batch := genFakeBatch(preffix, BATCH_SIZE)
	if todo < 0 {
		return counter, fmt.Errorf("ой всё!")
	}
	if cow, err := runPipeline(batch, fn, rdb, ctx); err == nil {
		counter += cow
		return counter, nil
	} else {
		return counter, err
	}
}

func pipelineHt(iterations int, rdb redis.Client, minuteFraction float32, hashsize int, delay int) (int, error) {
	ctx := context.Background()
	counter := 0
	batchCounter := 0
	minutePart := int(float32(hashsize) * minuteFraction)
	var exFun expirator
	exFun = func() time.Duration {
		return time.Duration(delay)*time.Minute + time.Duration(rand.IntN(60))*time.Second
	}
	fmt.Println("minutepart: ", minutePart)
	pipe := rdb.Pipeline()
	for counter < iterations {
		hItem := genFakeHS(hashsize)
		pipe.HSet(ctx, hItem.key, hItem.fields)
		fieldIndex := 0
		for k, _ := range hItem.fields {
			if fieldIndex <= minutePart {
				pipe.HExpire(ctx, hItem.key, exFun(), k)
			} else {
				pipe.HExpire(ctx, hItem.key, const24Hour(), k)
			}
			fieldIndex++
			counter++
		}
		batchCounter += len(hItem.fields)
		if batchCounter >= BATCH_SIZE {
			if _, err := pipe.Exec(ctx); err != nil {
				return counter, err
			}
			batchCounter = 0
			fmt.Println("loaded (ht): ", counter)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return counter, err
	}
	return counter, nil
}
func avgHsetLen(sampleCount int, rdb redis.Client) (float32, error) {
	ctx := context.Background()
	var res int64 = 0
	pipe := rdb.Pipeline()
	for i := 0; i < sampleCount; i++ {
		pipe.RandomKey(ctx)
	}
	if keys, err := pipe.Exec(ctx); err != nil {
		return 0, err
	} else {
		pipeLen := rdb.Pipeline()
		for _, item := range keys {
			if item.Err() != nil {
				return 0, err
			} else {
				key := item.(*redis.StringCmd).Val()
				pipeLen.HLen(ctx, key)
			}
		}
		if lengths, err := pipeLen.Exec(ctx); err != nil {
			return 0, err
		} else {
			for _, item := range lengths {
				if item.Err() != nil {
					return 0, item.Err()
				} else {
					len := item.(*redis.IntCmd).Val()
					res += len
				}
			}
			return float32(res) / float32(sampleCount), nil
		}
	}
}
