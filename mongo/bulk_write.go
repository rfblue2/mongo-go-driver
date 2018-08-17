// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongo

import (
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/core/command"
	"github.com/mongodb/mongo-go-driver/core/option"
	"github.com/mongodb/mongo-go-driver/mongo/deleteopt"
	"github.com/mongodb/mongo-go-driver/mongo/replaceopt"
	"github.com/mongodb/mongo-go-driver/mongo/updateopt"
)

// WriteModel is a marker interface for writes that can be batched together.
type WriteModel interface {
	writeModel()
	ConvertModel(bypassDocValidation bool) (command.WriteModel, error)
}

// InsertOneModel specifies an insertOne operation in a bulk write.
type InsertOneModel struct {
	// Document is the document to insert.
	Document interface{}
}

func (*InsertOneModel) writeModel() {}

// ConvertModel converts the InsertOneModel into a command.InsertModel. For internal use only.
func (iom *InsertOneModel) ConvertModel(bypassDocValidation bool) (command.WriteModel, error) {
	var opts []option.InsertOptioner
	if bypassDocValidation {
		opts = append(opts, option.OptBypassDocumentValidation(bypassDocValidation))
	}

	doc, err := TransformDocument(iom.Document)
	if err != nil {
		return nil, err
	}

	return &command.InsertModel{
		Docs:    []*bson.Document{doc},
		Options: opts,
	}, nil
}

// DeleteOneModel specifies a deleteOne operation in a bulk write.
type DeleteOneModel struct {
	// The filter to limit the deleted documents.
	Filter interface{}
	// Options are the delete options.
	Options []deleteopt.Delete
}

func (*DeleteOneModel) writeModel() {}

// ConvertModel converts the DeleteOneModel into a command.DeleteModel. For internal use only.
func (dom *DeleteOneModel) ConvertModel(bypassDocValidation bool) (command.WriteModel, error) {
	opts, _, err := deleteopt.BundleDelete(dom.Options...).Unbundle(true)
	if err != nil {
		return nil, err
	}

	f, err := TransformDocument(dom.Filter)
	if err != nil {
		return nil, err
	}
	deleteDocs := []*bson.Document{
		bson.NewDocument(
			bson.EC.SubDocument("q", f),
			bson.EC.Int32("limit", 1)),
	}

	return &command.DeleteModel{
		Deletes: deleteDocs,
		Options: opts,
	}, nil
}

// DeleteManyModel specifies a deleteMany operation in a bulk write.
type DeleteManyModel struct {
	// The filter to limit the deleted documents.
	Filter interface{}
	// Options are the delete options.
	Options []deleteopt.Delete
}

func (*DeleteManyModel) writeModel() {}

// ConvertModel converts the DeleteManyModel into a command.DeleteModel. For internal use only.
func (dmm *DeleteManyModel) ConvertModel(bypassDocValidation bool) (command.WriteModel, error) {
	opts, _, err := deleteopt.BundleDelete(dmm.Options...).Unbundle(true)
	if err != nil {
		return nil, err
	}

	f, err := TransformDocument(dmm.Filter)
	if err != nil {
		return nil, err
	}
	deleteDocs := []*bson.Document{bson.NewDocument(bson.EC.SubDocument("q", f), bson.EC.Int32("limit", 0))}

	return &command.DeleteModel{
		Deletes: deleteDocs,
		Options: opts,
	}, nil
}

// ReplaceOneModel specifies a replaceOne operation in a bulk write.
type ReplaceOneModel struct {
	// Filter is the filter to limit the replaced document.
	Filter interface{}
	// Replacement is the document with which to replace the matched document.
	Replacement interface{}
	// Options are the replace options.
	Options []replaceopt.Replace
}

func (*ReplaceOneModel) writeModel() {}

// ConvertModel converts the ReplaceOneModel into a command.UpdateModel. For internal use only.
func (rom *ReplaceOneModel) ConvertModel(bypassDocValidation bool) (command.WriteModel, error) {
	replaceOpts, _, err := replaceopt.BundleReplace(rom.Options...).Unbundle(true)
	if err != nil {
		return nil, err
	}
	var updateOpts []option.UpdateOptioner
	for _, opt := range replaceOpts {
		updateOpts = append(updateOpts, opt)
	}
	if bypassDocValidation {
		updateOpts = append(updateOpts, option.OptBypassDocumentValidation(bypassDocValidation))
	}

	f, err := TransformDocument(rom.Filter)
	if err != nil {
		return nil, err
	}

	r, err := TransformDocument(rom.Replacement)
	if err != nil {
		return nil, err
	}

	updateDocs := []*bson.Document{
		bson.NewDocument(
			bson.EC.SubDocument("q", f),
			bson.EC.SubDocument("u", r),
			bson.EC.Boolean("multi", false),
		),
	}

	return &command.UpdateModel{
		Docs:    updateDocs,
		Options: updateOpts,
	}, nil
}

// UpdateOneModel specifies an updateOne operation in a bulk write.
type UpdateOneModel struct {
	// Filter is the filter to limit the updated documents.
	Filter interface{}
	// Doc is a document containing update operators.
	Update interface{}
	// Options are the update options.
	Options []updateopt.Update
}

func (*UpdateOneModel) writeModel() {}

// ConvertModel converts the UpdateOneModel into a command.UpdateModel. For internal use only.
func (uom *UpdateOneModel) ConvertModel(bypassDocValidation bool) (command.WriteModel, error) {
	opts, _, err := updateopt.BundleUpdate(uom.Options...).Unbundle(true)
	if err != nil {
		return nil, err
	}
	if bypassDocValidation {
		opts = append(opts, option.OptBypassDocumentValidation(bypassDocValidation))
	}

	filter, err := TransformDocument(uom.Filter)
	if err != nil {
		return nil, err
	}

	update, err := TransformDocument(uom.Update)
	if err != nil {
		return nil, err
	}

	if err := ensureDollarKey(update); err != nil {
		return nil, err
	}

	updateDocs := []*bson.Document{
		bson.NewDocument(
			bson.EC.SubDocument("q", filter),
			bson.EC.SubDocument("u", update),
			bson.EC.Boolean("multi", false),
		),
	}

	return &command.UpdateModel{
		Docs:    updateDocs,
		Options: opts,
	}, nil
}

// UpdateManyModel specifies an updateMany operation in a bulk write.
type UpdateManyModel struct {
	// Filter is the filter to limit the updated documents.
	Filter interface{}
	// Doc is a document containing update operators.
	Update interface{}
	// Options are the update options.
	Options []updateopt.Update
}

func (*UpdateManyModel) writeModel() {}

// ConvertModel converts the UpdateManyModel into a command.UpdateModel. For internal use only.
func (umm *UpdateManyModel) ConvertModel(bypassDocValidation bool) (command.WriteModel, error) {
	opts, _, err := updateopt.BundleUpdate(umm.Options...).Unbundle(true)
	if err != nil {
		return nil, err
	}
	if bypassDocValidation {
		opts = append(opts, option.OptBypassDocumentValidation(bypassDocValidation))
	}

	f, err := TransformDocument(umm.Filter)
	if err != nil {
		return nil, err
	}

	u, err := TransformDocument(umm.Update)
	if err != nil {
		return nil, err
	}

	if err = ensureDollarKey(u); err != nil {
		return nil, err
	}

	updateDocs := []*bson.Document{
		bson.NewDocument(
			bson.EC.SubDocument("q", f),
			bson.EC.SubDocument("u", u),
			bson.EC.Boolean("multi", true),
		),
	}

	return &command.UpdateModel{
		Docs:    updateDocs,
		Options: opts,
	}, nil
}
