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

// Insert represents the insert command.
//
// The insert command inserts a set of documents into the database.
//
// Since the Insert command does not return any value other than ok or
// an error, this type has no Err method.
type Insert struct {
	Clock        *session.ClusterClock
	NS           Namespace
	Docs         []*bson.Document
	Opts         []option.InsertOptioner
	WriteConcern *writeconcern.WriteConcern
	Session      *session.Client

	batches         []*Write
	result          result.Insert
	err             error
	continueOnError bool
}

// Encode will encode this command into a wire message for the given server description.
func (i *Insert) Encode(desc description.SelectedServer) ([]wiremessage.WireMessage, error) {
	err := i.encode(desc)
	if err != nil {
		return nil, err
	}

	return batchesToWireMessage(i.batches, desc)
}

func (i *Insert) encodeBatch(docs []*bson.Document, desc description.SelectedServer) (*Write, error) {
	options := make([]option.Optioner, len(i.Opts))
	for j, opt := range i.Opts {
		options[j] = opt
	}

	command, err := encodeBatch(docs, options, i.NS.Collection, insertCommand)
	if err != nil {
		return nil, err
	}

	for _, opt := range i.Opts {
		if ordered, ok := opt.(option.OptOrdered); ok {
			if !ordered {
				i.continueOnError = true
			}
		}
	}

	return &Write{
		Clock:        i.Clock,
		DB:           i.NS.DB,
		Command:      command,
		WriteConcern: i.WriteConcern,
		Session:      i.Session,
	}, nil
}

func (i *Insert) encode(desc description.SelectedServer) error {
	batches, err := split(int(desc.MaxBatchCount), int(desc.MaxDocumentSize), i.Docs)
	if err != nil {
		return err
	}

	for _, docs := range batches {
		cmd, err := i.encodeBatch(docs, desc)
		if err != nil {
			return err
		}

		i.batches = append(i.batches, cmd)
	}
	return nil
}

// Decode will decode the wire message using the provided server description. Errors during decoding
// are deferred until either the Result or Err methods are called.
func (i *Insert) Decode(desc description.SelectedServer, wm wiremessage.WireMessage) *Insert {
	rdr, err := (&Write{}).Decode(desc, wm).Result()
	if err != nil {
		i.err = err
		return i
	}

	return i.decode(desc, rdr)
}

func (i *Insert) decode(desc description.SelectedServer, rdr bson.Reader) *Insert {
	i.err = bson.Unmarshal(rdr, &i.result)
	return i
}

// Result returns the result of a decoded wire message and server description.
func (i *Insert) Result() (result.Insert, error) {
	if i.err != nil {
		return result.Insert{}, i.err
	}
	return i.result, nil
}

// Err returns the error set on this command.
func (i *Insert) Err() error { return i.err }

// RoundTrip handles the execution of this command using the provided wiremessage.ReadWriter.
func (i *Insert) RoundTrip(ctx context.Context, desc description.SelectedServer, rw wiremessage.ReadWriter) (result.Insert, error) {
	if i.batches == nil {
		err := i.encode(desc)
		if err != nil {
			return result.Insert{}, err
		}
	}

	r, batches, err := roundTripBatches(
		ctx, desc, rw,
		i.batches,
		i.continueOnError,
		i.Session,
		insertCommand,
	)

	// if there are leftover batches, save them for retry
	if batches != nil {
		i.batches = batches
	}

	if err != nil {
		return result.Insert{}, err
	}

	res := r.(result.Insert)

	return res, nil
}
