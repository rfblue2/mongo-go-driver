// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package dispatch

import (
	"context"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/core/command"
	"github.com/mongodb/mongo-go-driver/core/description"
	"github.com/mongodb/mongo-go-driver/core/option"
	"github.com/mongodb/mongo-go-driver/core/result"
	"github.com/mongodb/mongo-go-driver/core/session"
	"github.com/mongodb/mongo-go-driver/core/topology"
	"github.com/mongodb/mongo-go-driver/core/uuid"
	"github.com/mongodb/mongo-go-driver/core/writeconcern"
)

// BulkWrite handles the full cycle dispatch and execution of a bulkWrite command
// against the provided topology.
func BulkWrite(
	ctx context.Context,
	cmd command.BulkWrite,
	topo *topology.Topology,
	selector description.ServerSelector,
	clientID uuid.UUID,
	pool *session.Pool,
	retryWrite bool,
) (result.BulkWrite, error) {
	if !cmd.Ordered {
		cmd.Models = groupBulkWrites(cmd.Models)
	}

	ss, err := topo.SelectServer(ctx, selector)
	if err != nil {
		return result.BulkWrite{}, err
	}

	// If no explicit session and deployment supports sessions, start implicit session.
	if cmd.Session == nil && topo.SupportsSessions() {
		cmd.Session, err = session.NewClientSession(pool, clientID, session.Implicit)
		if err != nil {
			return result.BulkWrite{}, err
		}
		defer cmd.Session.EndSession()
	}

	// Execute in a single trip if retry writes not supported, or retry not enabled
	if !retrySupported(topo, ss.Description(), cmd.Session, cmd.WriteConcern) || !retryWrite {
		if cmd.Session != nil {
			cmd.Session.RetryWrite = false // explicitly set to false to prevent encoding transaction number
		}
		return bulkWrite(ctx, cmd, ss, nil)
	}

	cmd.Session.RetryWrite = retryWrite
	cmd.Session.IncrementTxnNumber()

	res, originalErr := bulkWrite(ctx, cmd, ss, nil)

	// Retry if appropriate
	if cerr, ok := originalErr.(command.Error); ok && cerr.Retryable() ||
		res.WriteConcernError != nil && command.IsWriteConcernErrorRetryable(res.WriteConcernError) {
		ss, err := topo.SelectServer(ctx, selector)

		// Return original error if server selection fails or new server does not support retryable writes
		if err != nil || !retrySupported(topo, ss.Description(), cmd.Session, cmd.WriteConcern) {
			return res, originalErr
		}

		return bulkWrite(ctx, cmd, ss, cerr)
	}

	return res, originalErr
}

func bulkWrite(
	ctx context.Context,
	cmd command.BulkWrite,
	ss *topology.SelectedServer,
	oldErr error,
) (result.BulkWrite, error) {
	desc := ss.Description()
	conn, err := ss.Connection(ctx)
	if err != nil {
		if oldErr != nil {
			return result.BulkWrite{}, oldErr
		}
		return result.BulkWrite{}, err
	}

	if !writeconcern.AckWrite(cmd.WriteConcern) {
		go func() {
			defer func() { _ = recover() }()
			defer conn.Close()

			_, _ = cmd.RoundTrip(ctx, desc, conn)
		}()

		return result.BulkWrite{}, command.ErrUnacknowledgedWrite
	}
	defer conn.Close()

	return cmd.RoundTrip(ctx, desc, conn)
}

// Optimizes requests by grouping together commands
func groupBulkWrites(models []command.WriteModel) []command.WriteModel {
	var newModels []command.WriteModel
	var insertDocs []*bson.Document
	var insertOpts []option.InsertOptioner
	var updateDocs []*bson.Document
	var updateOpts []option.UpdateOptioner
	var deleteDocs []*bson.Document
	var deleteOpts []option.DeleteOptioner

	for _, model := range models {
		switch conv := model.(type) {
		case command.InsertModel:
			for _, doc := range conv.Docs {
				insertDocs = append(insertDocs, doc)
			}
			if insertOpts == nil {
				insertOpts = conv.Options
			}
		case command.UpdateModel:
			for _, doc := range conv.Docs {
				updateDocs = append(updateDocs, doc)
			}
			if updateOpts == nil {
				updateOpts = conv.Options
			}
		case command.DeleteModel:
			for _, doc := range conv.Deletes {
				deleteDocs = append(deleteDocs, doc)
			}
			if deleteOpts == nil {
				deleteOpts = conv.Options
			}
		default:
			newModels = append(newModels, model)
		}
	}

	if len(insertDocs) > 0 {
		newInsertDoc := command.InsertModel{Docs: insertDocs}
		if insertOpts != nil {
			newInsertDoc.Options = insertOpts
		}
		newModels = append(newModels, newInsertDoc)
	}

	if len(updateDocs) > 0 {
		newUpdateDoc := command.UpdateModel{Docs: updateDocs}
		if updateOpts != nil {
			newUpdateDoc.Options = updateOpts
		}
		newModels = append(newModels, newUpdateDoc)
	}

	if len(deleteDocs) > 0 {
		newDeleteDoc := command.DeleteModel{Deletes: deleteDocs}
		if deleteOpts != nil {
			newDeleteDoc.Options = deleteOpts
		}
		newModels = append(newModels, newDeleteDoc)
	}

	return newModels
}
