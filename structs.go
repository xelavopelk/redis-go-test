package main

import (
	"fmt"
	"math/rand"
	"time"
)

const KEY_SIZE int = 20

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString(n int, randomizer rand.Rand) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[randomizer.Intn(len(letters))]
	}
	return string(b)
}

func genFakeBatch(prefix string, size int) map[string]string {
	source := rand.NewSource(time.Now().UnixNano())
	rndGen := rand.New(source)
	res := make(map[string]string, size)
	for i := 0; i < size; i++ {
		res[prefix+":"+randString(KEY_SIZE, *rndGen)] = fmt.Sprintf("%d", rndGen.Int63())
	}
	return res
}

type FakeHs struct {
	key    string
	fields map[string]string
}

func genFakeHS(sizeFields int) FakeHs {
	source := rand.NewSource(time.Now().UnixNano())
	rnd := rand.New(source)
	fields := make(map[string]string, sizeFields)
	for i := 0; i < sizeFields; i++ {
		fields[randString(KEY_SIZE/2, *rnd)] = fmt.Sprintf("%d", rnd.Int63())
	}
	return FakeHs{key: randString(KEY_SIZE/2, *rnd), fields: fields}
}
