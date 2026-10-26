package particledb

import (
	"context"
	"testing"
)

// go test -run x -bench . with PDB_TEST_DSN set (bench_kv from the compare
// benchmark must exist).
func BenchmarkLivePointSelect(b *testing.B) {
	c := liveConn(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var v string
		if err := c.QueryRow(ctx, "SELECT v FROM bench_kv WHERE id = $1", int64(i%100000)).Scan(&v); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLivePointSelectPool(b *testing.B) {
	p := livePool(b, "")
	ctx := context.Background()
	b.ReportAllocs()
	b.SetParallelism(14)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			var v string
			if err := p.QueryRow(ctx, "SELECT v FROM bench_kv WHERE id = $1", int64(i%100000)).Scan(&v); err != nil {
				b.Error(err)
				return
			}
			i++
		}
	})
}
