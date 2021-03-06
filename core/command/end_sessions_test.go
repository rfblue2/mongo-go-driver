package command

import (
	"testing"

	"github.com/mongodb/mongo-go-driver/bson"
)

func TestEndSessions(t *testing.T) {
	t.Run("TestSplitBatches", func(t *testing.T) {
		ids := []*bson.Document{}
		for i := 0; i < 2*BatchSize; i++ {
			ids = append(ids, bson.NewDocument(
				bson.EC.Int32("x", int32(i)),
			))
		}

		es := &EndSessions{
			SessionIDs: ids,
		}

		batches := es.split()
		if len(batches) != 2 {
			t.Fatalf("incorrect number of batches. expected 2 got %d", len(batches))
		}

		for i, batch := range batches {
			if len(batch) != BatchSize {
				t.Fatalf("incorrect batch size for batch %d. expected %d got %d", i, BatchSize, len(batch))
			}
		}
	})
}
