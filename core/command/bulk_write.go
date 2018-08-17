// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package command

import (
	"context"
	"fmt"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/core/description"
	"github.com/mongodb/mongo-go-driver/core/option"
	"github.com/mongodb/mongo-go-driver/core/result"
	"github.com/mongodb/mongo-go-driver/core/session"
	"github.com/mongodb/mongo-go-driver/core/wiremessage"
	"github.com/mongodb/mongo-go-driver/core/writeconcern"
)

// WriteModel is a marker interface for bulk writes.
type WriteModel interface {
	writeModel()
}

// InsertModel specifies an insert operation in a bulk write.
type InsertModel struct {
	Docs    []*bson.Document
	Options []option.InsertOptioner
}

func (InsertModel) writeModel() {}

// DeleteModel specifies a delete operation in a bulk write.
type DeleteModel struct {
	Deletes []*bson.Document
	Options []option.DeleteOptioner
}

func (DeleteModel) writeModel() {}

// UpdateModel specifies an update operation in a bulk write.
type UpdateModel struct {
	Docs    []*bson.Document
	Options []option.UpdateOptioner
}

func (UpdateModel) writeModel() {}

type encodedCommand struct {
	encoded *Write
	kind    commandKind
}

// commandKind is the type of command that the Write command represents
type commandKind int8

const (
	insertCommand commandKind = iota
	updateCommand
	deleteCommand
)

// BulkWrite represents the bulkwrite command.
type BulkWrite struct {
	NS           Namespace
	Models       []WriteModel
	Ordered      bool
	WriteConcern *writeconcern.WriteConcern
	Session      *session.Client
	Clock        *session.ClusterClock
	RetryWrite   bool

	batches []*encodedCommand
	result  result.BulkWrite
	err     error
}

// Encode will encode this command into a wire message for the given server description.
func (bw *BulkWrite) Encode(desc description.SelectedServer) ([]wiremessage.WireMessage, error) {
	err := bw.encode(desc)
	if err != nil {
		return nil, err
	}

	var encodedWms []wiremessage.WireMessage
	for _, encodedCmd := range bw.batches {
		wm, err := encodedCmd.encoded.Encode(desc)
		if err != nil {
			return nil, err
		}
		encodedWms = append(encodedWms, wm)
	}

	return encodedWms, nil
}

func (bw *BulkWrite) encode(desc description.SelectedServer) error {
	var encodedCmds []*encodedCommand
	for _, model := range bw.Models {
		switch conv := model.(type) {
		case InsertModel:
			insertCmd := &Insert{
				NS:           bw.NS,
				Docs:         conv.Docs,
				Opts:         conv.Options,
				WriteConcern: bw.WriteConcern,
				Session:      bw.Session,
				Clock:        bw.Clock,
			}
			err := insertCmd.encode(desc)
			if err != nil {
				return err
			}
			for _, batch := range insertCmd.batches {
				encodedCmds = append(encodedCmds, &encodedCommand{encoded: batch, kind: insertCommand})
			}
		case UpdateModel:
			updateCmd := &Update{
				NS:           bw.NS,
				Docs:         conv.Docs,
				Opts:         conv.Options,
				WriteConcern: bw.WriteConcern,
				Session:      bw.Session,
				Clock:        bw.Clock,
			}
			err := updateCmd.encode(desc)
			if err != nil {
				return err
			}
			for _, batch := range updateCmd.batches {
				encodedCmds = append(encodedCmds, &encodedCommand{encoded: batch, kind: insertCommand})
			}
		case DeleteModel:
			deleteCmd := &Delete{
				NS:           bw.NS,
				Deletes:      conv.Deletes,
				Opts:         conv.Options,
				WriteConcern: bw.WriteConcern,
				Session:      bw.Session,
				Clock:        bw.Clock,
			}
			err := deleteCmd.encode(desc)
			if err != nil {
				return err
			}
			for _, batch := range deleteCmd.batches {
				encodedCmds = append(encodedCmds, &encodedCommand{encoded: batch, kind: insertCommand})
			}
		default:
			return fmt.Errorf("unsupported bulk write model type")
		}
	}

	bw.batches = encodedCmds
	return nil
}

// Decode will decode the wire message using the provided server description. Errors during decoding
// are deferred until either the Result or Err methods are called.
func (bw *BulkWrite) Decode(desc description.SelectedServer, wm wiremessage.WireMessage) *BulkWrite {
	// TODO determine how to decode without knowing command type
	return nil
}

func (bw *BulkWrite) decode(desc description.SelectedServer, rdr bson.Reader, kind commandKind) *BulkWrite {
	// decode using individual command decodes and add the info to the aggregate result
	switch kind {
	case insertCommand:
		insertRes := &Insert{}
		insertRes.decode(desc, rdr)
		// TODO determine how to update insertedIDs (insertMany does it in collection.go)
		bw.result.InsertedCount += int64(insertRes.result.N)
	case updateCommand:
		updateRes := &Update{}
		updateRes.decode(desc, rdr)
		bw.result.ModifiedCount += updateRes.result.ModifiedCount
		bw.result.MatchedCount += updateRes.result.MatchedCount
		// TODO determine how to get upsertedIDs and upsertedCount (also in collection.go)
	case deleteCommand:
		deleteRes := &Delete{}
		deleteRes.decode(desc, rdr)
		bw.result.DeletedCount += int64(deleteRes.result.N)
	}
	return bw
}

// Result returns the result of a decoded wire message and server description.
func (bw *BulkWrite) Result() (result.BulkWrite, error) {
	if bw.err != nil {
		return result.BulkWrite{}, bw.err
	}
	return bw.result, nil
}

// Err returns the error set on this command.
func (bw *BulkWrite) Err() error {
	return bw.err
}

// RoundTrip handles the execution of this command using the provided wiremessage.ReadWriter.
func (bw *BulkWrite) RoundTrip(ctx context.Context, desc description.SelectedServer, rw wiremessage.ReadWriter) (result.BulkWrite, error) {
	res := result.BulkWrite{}
	if bw.batches == nil {
		err := bw.encode(desc)
		if err != nil {
			return res, err
		}
	}

	// TODO consolidate with insert?
	// hold on to txnNumber, reset it when loop exits to ensure reuse of same
	// transaction number if retry is needed
	var txnNumber int64
	if bw.Session != nil && bw.Session.RetryWrite {
		txnNumber = bw.Session.TxnNumber
	}
	for i, cmd := range bw.batches {
		rdr, err := cmd.encoded.RoundTrip(ctx, desc, rw)
		if err != nil {
			if bw.Session != nil && bw.Session.RetryWrite {
				bw.Session.TxnNumber = txnNumber + int64(i)
			}
			return res, err
		}

		r, err := bw.decode(desc, rdr, cmd.kind).Result()
		if err != nil {
			return res, err
		}

		if r.WriteConcernError != nil {
			res.WriteConcernError = r.WriteConcernError
			if bw.Session != nil && bw.Session.RetryWrite {
				bw.Session.TxnNumber = txnNumber
				return res, nil // report writeconcernerror for retry
			}
		}

		// Increment txnNumber for each batch
		if bw.Session != nil && bw.Session.RetryWrite {
			bw.Session.IncrementTxnNumber()
			bw.batches = bw.batches[1:] // if batch encoded successfully, remove it from the slice
		}
	}

	return res, nil
}
