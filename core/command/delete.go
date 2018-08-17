// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package command

import (
	"context"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/core/description"
	"github.com/mongodb/mongo-go-driver/core/option"
	"github.com/mongodb/mongo-go-driver/core/result"
	"github.com/mongodb/mongo-go-driver/core/session"
	"github.com/mongodb/mongo-go-driver/core/wiremessage"
	"github.com/mongodb/mongo-go-driver/core/writeconcern"
)

// Delete represents the delete command.
//
// The delete command executes a delete with a given set of delete documents
// and options.
type Delete struct {
	NS           Namespace
	Deletes      []*bson.Document
	Opts         []option.DeleteOptioner
	WriteConcern *writeconcern.WriteConcern
	Clock        *session.ClusterClock
	Session      *session.Client

	batches []*Write
	result  result.Delete
	err     error
}

// Encode will encode this command into a wire message for the given server description.
func (d *Delete) Encode(desc description.SelectedServer) ([]wiremessage.WireMessage, error) {
	err := d.encode(desc)
	if err != nil {
		return nil, err
	}

	return batchesToWireMessage(d.batches, desc)
}

func (d *Delete) encode(desc description.SelectedServer) error {
	batches, err := split(int(desc.MaxBatchCount), int(desc.MaxDocumentSize), d.Deletes)
	if err != nil {
		return err
	}

	for _, docs := range batches {
		cmd, err := d.encodeBatch(docs, desc)
		if err != nil {
			return err
		}

		d.batches = append(d.batches, cmd)
	}

	return nil
}

func (d *Delete) encodeBatch(docs []*bson.Document, desc description.SelectedServer) (*Write, error) {

	copyDocs := make([]*bson.Document, 0, len(docs)) // copy of all the documents
	for _, doc := range docs {
		newDoc := doc.Copy()
		copyDocs = append(copyDocs, newDoc)
	}

	var options []option.Optioner
	for _, opt := range d.Opts {
		switch opt.(type) {
		case nil:
			continue
		case option.OptCollation:
			// options that are encoded on each individual document
			for _, doc := range copyDocs {
				err := opt.Option(doc)
				if err != nil {
					return nil, err
				}
			}
		default:
			options = append(options, opt)
		}
	}

	command, err := encodeBatch(copyDocs, options, d.NS.Collection, deleteCommand)
	if err != nil {
		return nil, err
	}

	return &Write{
		Clock:        d.Clock,
		DB:           d.NS.DB,
		Command:      command,
		WriteConcern: d.WriteConcern,
		Session:      d.Session,
	}, nil
}

// Decode will decode the wire message using the provided server description. Errors during decoding
// are deferred until either the Result or Err methods are called.
func (d *Delete) Decode(desc description.SelectedServer, wm wiremessage.WireMessage) *Delete {
	rdr, err := (&Write{}).Decode(desc, wm).Result()
	if err != nil {
		d.err = err
		return d
	}

	return d.decode(desc, rdr)
}

func (d *Delete) decode(desc description.SelectedServer, rdr bson.Reader) *Delete {
	d.err = bson.Unmarshal(rdr, &d.result)
	return d
}

// Result returns the result of a decoded wire message and server description.
func (d *Delete) Result() (result.Delete, error) {
	if d.err != nil {
		return result.Delete{}, d.err
	}
	return d.result, nil
}

// Err returns the error set on this command.
func (d *Delete) Err() error { return d.err }

// RoundTrip handles the execution of this command using the provided wiremessage.ReadWriter.
func (d *Delete) RoundTrip(ctx context.Context, desc description.SelectedServer, rw wiremessage.ReadWriter) (result.Delete, error) {
	if d.batches == nil {
		err := d.encode(desc)
		if err != nil {
			return result.Delete{}, err
		}
	}

	r, batches, err := roundTripBatches(
		ctx, desc, rw,
		d.batches,
		false, // TODO get this from bulk write opt somehow
		d.Session,
		deleteCommand,
	)

	// if there are leftover batches, save them for retry
	if batches != nil {
		d.batches = batches
	}

	if err != nil {
		return result.Delete{}, err
	}

	res := r.(result.Delete)

	return res, nil
}
