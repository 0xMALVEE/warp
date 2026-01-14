package bench

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/minio/warp/pkg/iceberg"
	"github.com/minio/warp/pkg/iceberg/rest"
)

type WeightedDistribution struct {
	Count    int
	Mean     float64
	Variance float64
}

type IcebergWeighted struct {
	Common
	RestClient *rest.Client
	Tree       *iceberg.Tree
	TreeConfig iceberg.TreeConfig

	Readers []WeightedDistribution
	Writers []WeightedDistribution
	Seed    int64

	tables []iceberg.TableInfo
}

func (b *IcebergWeighted) Prepare(ctx context.Context) error {
	b.Tree = iceberg.NewTree(b.TreeConfig)

	b.UpdateStatus(fmt.Sprintf("Loading dataset info: %d tables", b.Tree.TotalTables()))

	b.tables = b.Tree.AllTables()

	if len(b.tables) == 0 {
		return fmt.Errorf("no tables found: check tree configuration")
	}

	b.UpdateStatus("Verifying catalog connectivity...")
	catalog := b.TreeConfig.CatalogName

	_, err := b.RestClient.GetTable(ctx, catalog, b.tables[0].Namespace, b.tables[0].Name)
	if err != nil {
		return fmt.Errorf("cannot access table: %w", err)
	}

	b.UpdateStatus(fmt.Sprintf("Preparation complete - %d tables available for weighted workload", len(b.tables)))
	return nil
}

func (b *IcebergWeighted) Start(ctx context.Context, wait chan struct{}) error {
	var wg sync.WaitGroup
	c := b.Collector

	if b.AutoTermDur > 0 {
		ctx = c.AutoTerm(ctx, OpTableGet, b.AutoTermScale, autoTermCheck, autoTermSamples, b.AutoTermDur)
	}

	threadID := 0

	for distIdx, dist := range b.Readers {
		for i := 0; i < dist.Count; i++ {
			wg.Add(1)
			seed := b.Seed + int64((distIdx+1)*1000) + int64(threadID)
			go func(thread int, d WeightedDistribution, s int64) {
				defer wg.Done()
				b.runReader(ctx, wait, thread, d, s)
			}(threadID, dist, seed)
			threadID++
		}
	}

	for distIdx, dist := range b.Writers {
		for i := 0; i < dist.Count; i++ {
			wg.Add(1)
			seed := b.Seed + int64((distIdx+1)*2000) + int64(threadID)
			go func(thread int, d WeightedDistribution, s int64) {
				defer wg.Done()
				b.runWriter(ctx, wait, thread, d, s)
			}(threadID, dist, seed)
			threadID++
		}
	}

	wg.Wait()
	return nil
}

func (b *IcebergWeighted) runReader(ctx context.Context, wait chan struct{}, thread int, dist WeightedDistribution, seed int64) {
	rcv := b.Collector.Receiver()
	done := ctx.Done()
	catalog := b.TreeConfig.CatalogName
	rng := rand.New(rand.NewSource(seed))

	<-wait

	for {
		select {
		case <-done:
			return
		default:
		}

		if b.rpsLimit(ctx) != nil {
			return
		}

		tableIdx := sampleTableIndex(rng, dist, len(b.tables))
		tbl := b.tables[tableIdx]

		op := Operation{
			OpType:   OpTableGet,
			Thread:   uint32(thread),
			File:     fmt.Sprintf("%s/%v/%s", catalog, tbl.Namespace, tbl.Name),
			ObjPerOp: 1,
			Endpoint: catalog,
		}

		op.Start = time.Now()
		_, err := b.RestClient.GetTable(ctx, catalog, tbl.Namespace, tbl.Name)
		op.End = time.Now()

		if err != nil {
			op.Err = err.Error()
		}
		rcv <- op
	}
}

func (b *IcebergWeighted) runWriter(ctx context.Context, wait chan struct{}, thread int, dist WeightedDistribution, seed int64) {
	rcv := b.Collector.Receiver()
	done := ctx.Done()
	catalog := b.TreeConfig.CatalogName
	rng := rand.New(rand.NewSource(seed))

	<-wait

	for {
		select {
		case <-done:
			return
		default:
		}

		if b.rpsLimit(ctx) != nil {
			return
		}

		tableIdx := sampleTableIndex(rng, dist, len(b.tables))
		tbl := b.tables[tableIdx]

		now := time.Now().UnixMilli()
		req := rest.CommitTableRequest{
			Updates: []rest.TableUpdate{
				{
					Action: "set-properties",
					Updates: map[string]string{
						"last_updated": fmt.Sprintf("%d", now),
					},
				},
			},
		}

		op := Operation{
			OpType:   OpTableUpdate,
			Thread:   uint32(thread),
			File:     fmt.Sprintf("%s/%v/%s", catalog, tbl.Namespace, tbl.Name),
			ObjPerOp: 1,
			Endpoint: catalog,
		}

		op.Start = time.Now()
		_, err := b.RestClient.UpdateTable(ctx, catalog, tbl.Namespace, tbl.Name, req)
		op.End = time.Now()

		if err != nil {
			op.Err = err.Error()
		}
		rcv <- op
	}
}

func sampleTableIndex(rng *rand.Rand, dist WeightedDistribution, numTables int) int {
	stddev := math.Sqrt(dist.Variance)
	const maxSamples = 100000

	var value float64
	for i := 0; i < maxSamples; i++ {
		sample := rng.NormFloat64()*stddev + dist.Mean
		if sample >= 0.0 && sample <= 1.0 {
			value = sample
			break
		}
	}

	idx := int(value * float64(numTables))
	if idx >= numTables {
		idx = numTables - 1
	}
	if idx < 0 {
		idx = 0
	}

	return idx
}

func (b *IcebergWeighted) Cleanup(_ context.Context) {
	b.UpdateStatus("Cleanup: skipping (weighted benchmark does not delete data)")
}
