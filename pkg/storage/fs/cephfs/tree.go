// Copyright 2018-2021 CERN
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// In applying this license, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

package cephfs

import (
	"context"
	cephfs2 "github.com/ceph/go-ceph/cephfs"
	"os"
	"path/filepath"
	"time"

	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/pkg/appctx"
	"github.com/cs3org/reva/pkg/errtypes"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// Tree manages a hierarchical tree
type Tree struct {
	lu *Lookup
	mt *cephfs2.MountInfo
}

// NewTree creates a new Tree instance
func NewTree(lu *Lookup, mt *cephfs2.MountInfo) (TreePersistence, error) {
	return &Tree{
		lu: lu,
		mt: mt,
	}, nil
}

// GetMD returns the metadata of a node in the tree
func (t *Tree) GetMD(_ context.Context, node *Node) (*cephfs2.CephStatx, error) {
	md, err := t.mt.Statx(t.lu.toInternalPath(node.ID), cephfs2.StatxBasicStats, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errtypes.NotFound(node.ID)
		}
		return nil, errors.Wrap(err, "tree: error stating "+node.ID)
	}

	return md, nil
}

// GetPathByID returns the fn pointed by the file id, without the internal namespace
func (t *Tree) GetPathByID(ctx context.Context, id *provider.ResourceId) (relativeExternalPath string, err error) {
	var node *Node
	node, err = t.lu.NodeFromID(ctx, id)
	if err != nil {
		return
	}

	relativeExternalPath, err = t.lu.Path(ctx, node)
	return
}

// does not take care of linking back to parent
// TODO check if node exists?
func createNode(n *Node, owner *userpb.UserId, mount *cephfs2.MountInfo) (err error) {
	// create a directory node
	nodePath := n.lu.toInternalPath(n.ID)

	if err = mount.MakeDir(nodePath, 0700); err != nil {
		return errors.Wrap(err, "mount: error creating node")
	}

	return n.writeMetadata(owner)
}

// CreateDir creates a new directory entry in the tree
func (t *Tree) CreateDir(ctx context.Context, node *Node) (err error) {
	if node.Exists || node.ID != "" {
		return errtypes.AlreadyExists(node.ID) // path?
	}

	// create a directory node
	node.ID = uuid.New().String()

	// who will become the owner? the owner of the parent node, not the current user
	var p *Node
	p, err = node.Parent()
	if err != nil {
		return
	}
	var owner *userpb.UserId
	owner, err = p.Owner()
	if err != nil {
		return
	}

	err = createNode(node, owner, t.mt)
	if err != nil {
		return nil
	}

	// make child appear in listings
	err = t.mt.Symlink("../"+node.ID, filepath.Join(t.lu.toInternalPath(node.ParentID), node.Name))
	if err != nil {
		return
	}
	return t.Propagate(ctx, node)
}

// Move replaces the target with the source
func (t *Tree) Move(ctx context.Context, oldNode *Node, newNode *Node) (err error) {
	// if target exists delete it without trashing it
	if newNode.Exists {
		// TODO make sure all children are deleted
		if err := t.mt.RemoveDir(t.lu.toInternalPath(newNode.ID)); err != nil {
			return errors.Wrap(err, "mount: Move: error deleting target node "+newNode.ID)
		}
	}

	// Always target the old node ID for xattr updates.
	// The new node id is empty if the target does not exist
	// and we need to overwrite the new one when overwriting an existing path.
	tgtPath := t.lu.toInternalPath(oldNode.ID)

	// are we just renaming (parent stays the same)?
	if oldNode.ParentID == newNode.ParentID {

		parentPath := t.lu.toInternalPath(oldNode.ParentID)

		// rename child
		err = t.mt.Rename(
			filepath.Join(parentPath, oldNode.Name),
			filepath.Join(parentPath, newNode.Name),
		)
		if err != nil {
			return errors.Wrap(err, "mount: could not rename child")
		}

		// update name attribute
		if err := t.mt.SetXattr(tgtPath, nameAttr, []byte(newNode.Name), 0); err != nil {
			return errors.Wrap(err, "mount: could not set name attribute")
		}

		return t.Propagate(ctx, newNode)
	}

	// we are moving the node to a new parent, any target has been removed
	// bring old node to the new parent

	// rename child
	err = t.mt.Rename(
		filepath.Join(t.lu.toInternalPath(oldNode.ParentID), oldNode.Name),
		filepath.Join(t.lu.toInternalPath(newNode.ParentID), newNode.Name),
	)
	if err != nil {
		return errors.Wrap(err, "mount: could not move child")
	}

	// update target parentid and name
	if err := t.mt.SetXattr(tgtPath, parentidAttr, []byte(newNode.ParentID), 0); err != nil {
		return errors.Wrap(err, "mount: could not set parentid attribute")
	}
	if err := t.mt.SetXattr(tgtPath, nameAttr, []byte(newNode.Name), 0); err != nil {
		return errors.Wrap(err, "mount: could not set name attribute")
	}

	// TODO inefficient because we might update several nodes twice, only propagate unchanged nodes?
	// collect in a list, then only stat each node once
	// also do this in a go routine ... webdav should check the etag async

	err = t.Propagate(ctx, oldNode)
	if err != nil {
		return errors.Wrap(err, "mount: Move: could not propagate old node")
	}
	err = t.Propagate(ctx, newNode)
	if err != nil {
		return errors.Wrap(err, "mount: Move: could not propagate new node")
	}
	return nil
}

// ListFolder lists the content of a folder node
func (t *Tree) ListFolder(_ context.Context, node *Node) ([]*Node, error) {
	dir := t.lu.toInternalPath(node.ID)
	f, err := t.mt.OpenDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errtypes.NotFound(dir)
		}
		return nil, errors.Wrap(err, "tree: error listing "+dir)
	}
	defer f.Close()

	var names []string
	var dentry *cephfs2.DirEntry
	for dentry, err = f.ReadDir(); err == nil && dentry != nil; dentry, err = f.ReadDir() {
		names = append(names, dentry.Name())
	}
	if err != nil {
		return nil, errors.Wrap(err, "tree: error listing children of "+dir)
	}
	var nodes []*Node
	for i := range names {
		link, err := t.mt.Readlink(filepath.Join(dir, names[i]))
		if err != nil {
			// TODO log
			continue
		}
		n := &Node{
			lu:       t.lu,
			ParentID: node.ID,
			ID:       filepath.Base(link),
			Name:     names[i],
			Exists:   true, // TODO
		}

		nodes = append(nodes, n)
	}
	return nodes, nil
}

// Delete deletes a node in the tree
func (t *Tree) Delete(ctx context.Context, n *Node) (err error) {

	// Prepare the trash
	// TODO use layout?, but it requires resolving the owners user if the username is used instead of the id.
	// the node knows the owner id so we use that for now
	o, err := n.Owner()
	if err != nil {
		return
	}
	if o.OpaqueId == "" {
		// fall back to root trash
		o.OpaqueId = "root"
	}
	err = t.mt.MakeDir(filepath.Join(t.lu.Options.Root, "trash", o.OpaqueId), 0700)
	if err != nil {
		return
	}

	// get the original path
	origin, err := t.lu.Path(ctx, n)
	if err != nil {
		return
	}

	// set origin location in metadata
	nodePath := t.lu.toInternalPath(n.ID)
	if err := t.mt.SetXattr(nodePath, trashOriginAttr, []byte(origin), 0); err != nil {
		return err
	}

	deletionTime := time.Now().UTC().Format(time.RFC3339Nano)

	// first make node appear in the owners (or root) trash
	// parent id and name are stored as extended attributes in the node itself
	trashLink := filepath.Join(t.lu.Options.Root, "trash", o.OpaqueId, n.ID)
	err = t.mt.Symlink("../../nodes/"+n.ID+".T."+deletionTime, trashLink)
	if err != nil {
		// To roll back changes
		// TODO unset trashOriginAttr
		return
	}

	// at this point we have a symlink pointing to a non existing destination, which is fine

	// rename the trashed node so it is not picked up when traversing up the tree and matches the symlink
	trashPath := nodePath + ".T." + deletionTime
	err = t.mt.Rename(nodePath, trashPath)
	if err != nil {
		// To roll back changes
		// TODO remove symlink
		// TODO unset trashOriginAttr
		return
	}

	// finally remove the entry from the parent dir
	src := filepath.Join(t.lu.toInternalPath(n.ParentID), n.Name)

	err = t.mt.Unlink(src)
	if err != nil {
		// To roll back changes
		// TODO revert the rename
		// TODO remove symlink
		// TODO unset trashOriginAttr
		return
	}

	p, err := n.Parent()
	if err != nil {
		return errors.Wrap(err, "mount: error getting parent "+n.ParentID)
	}
	return t.Propagate(ctx, p)
}

// Propagate propagates changes to the root of the tree
func (t *Tree) Propagate(ctx context.Context, n *Node) (err error) {
	sublog := appctx.GetLogger(ctx).With().Interface("node", n).Logger()
	if !t.lu.Options.TreeTimeAccounting && !t.lu.Options.TreeSizeAccounting {
		// no propagation enabled
		sublog.Debug().Msg("propagation disabled")
		return
	}

	// is propagation enabled for the parent node?

	var root *Node
	if root, err = t.lu.HomeOrRootNode(ctx); err != nil {
		return
	}

	// use a sync time and don't rely on the mtime of the current node, as the stat might not change when a rename happened too quickly
	sTime := time.Now().UTC()

	// we loop until we reach the root
	for err == nil && n.ID != root.ID {
		sublog.Debug().Msg("propagating")

		// make n the parent or break the loop
		if n, err = n.Parent(); err != nil {
			break
		}

		sublog = sublog.With().Interface("node", n).Logger()

		// TODO none, sync and async?
		if !n.HasPropagation() {
			sublog.Debug().Str("attr", propagationAttr).Msg("propagation attribute not set or unreadable, not propagating")
			// if the attribute is not set treat it as false / none / no propagation
			return nil
		}

		if t.lu.Options.TreeTimeAccounting {
			// update the parent tree time if it is older than the nodes mtime
			updateSyncTime := false

			var tmTime time.Time
			tmTime, err = n.GetTMTime()
			switch {
			case err != nil:
				// missing attribute, or invalid format, overwrite
				sublog.Debug().Err(err).Msg("could not read tmtime attribute, overwriting")
				updateSyncTime = true
			case tmTime.Before(sTime):
				sublog.Debug().
					Time("tmtime", tmTime).
					Time("stime", sTime).
					Msg("parent tmtime is older than node mtime, updating")
				updateSyncTime = true
			default:
				sublog.Debug().
					Time("tmtime", tmTime).
					Time("stime", sTime).
					Dur("delta", sTime.Sub(tmTime)).
					Msg("parent tmtime is younger than node mtime, not updating")
			}

			if updateSyncTime {
				// update the tree time of the parent node
				if err = n.SetTMTime(sTime); err != nil {
					sublog.Error().Err(err).Time("tmtime", sTime).Msg("could not update tmtime of parent node")
				} else {
					sublog.Debug().Time("tmtime", sTime).Msg("updated tmtime of parent node")
				}
			}

			if err := n.UnsetTempEtag(); err != nil {
				sublog.Error().Err(err).Msg("could not remove temporary etag attribute")
			}

		}

		// size accounting
		if t.lu.Options.TreeSizeAccounting {
			// update the treesize if it differs from the current size
			updateTreeSize := false

			var treeSize, calculatedTreeSize uint64
			calculatedTreeSize, err = n.CalculateTreeSize(ctx)
			if err != nil {
				continue
			}

			treeSize, err = n.GetTreeSize()
			switch {
			case err != nil:
				// missing attribute, or invalid format, overwrite
				sublog.Debug().Err(err).Msg("could not read treesize attribute, overwriting")
				updateTreeSize = true
			case treeSize != calculatedTreeSize:
				sublog.Debug().
					Uint64("treesize", treeSize).
					Uint64("calculatedTreeSize", calculatedTreeSize).
					Msg("parent treesize is different then calculated treesize, updating")
				updateTreeSize = true
			default:
				sublog.Debug().
					Uint64("treesize", treeSize).
					Uint64("calculatedTreeSize", calculatedTreeSize).
					Msg("parent size matches calculated size, not updating")
			}

			if updateTreeSize {
				// update the tree time of the parent node
				if err = n.SetTreeSize(calculatedTreeSize); err != nil {
					sublog.Error().Err(err).Uint64("calculatedTreeSize", calculatedTreeSize).Msg("could not update treesize of parent node")
				} else {
					sublog.Debug().Uint64("calculatedTreeSize", calculatedTreeSize).Msg("updated treesize of parent node")
				}
			}
		}
	}
	if err != nil {
		sublog.Error().Err(err).Msg("error propagating")
	}
	return
}
