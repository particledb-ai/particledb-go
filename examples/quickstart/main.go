package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/particledb-ai/particledb-go"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := particledb.NewPool(ctx, os.Getenv("PARTICLEDB_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	if err := pool.KVSet(ctx, []byte("session:123"), []byte("active")); err != nil {
		log.Fatal(err)
	}
	value, err := pool.KVGet(ctx, []byte("session:123"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(value))
}
