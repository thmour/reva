// Copyright 2018-2020 CERN
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
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"

	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	types "github.com/cs3org/go-cs3apis/cs3/types/v1beta1"
	"github.com/cs3org/reva/pkg/errtypes"
	"github.com/cs3org/reva/pkg/mime"
	"github.com/cs3org/reva/pkg/storage"
	"github.com/cs3org/reva/pkg/storage/utils/chunking"
	"github.com/cs3org/reva/pkg/storage/utils/grants"
	"github.com/cs3org/reva/pkg/storage/utils/templates"
	"github.com/cs3org/reva/pkg/user"
	"github.com/pkg/errors"
)

// Config holds the configuration details for the ceph fs.
type Config struct {
	Root          string `mapstructure:"root"`
	DisableHome   bool   `mapstructure:"disable_home"`
	UserLayout    string `mapstructure:"user_layout"`
	ShareFolder   string `mapstructure:"share_folder"`
	Uploads       string `mapstructure:"uploads"`
	DataDirectory string `mapstructure:"data_directory"`
	RecycleBin    string `mapstructure:"recycle_bin"`
	Shadow        string `mapstructure:"shadow"`
	References    string `mapstructure:"references"`
}

// Global config for snapshots
type Global struct {
	SnapIno string
	SnapDir string
}

func (c *Config) init() {
	if c.Root == "" {
		c.Root = "/var/tmp/reva"
	}

	if c.UserLayout == "" {
		c.UserLayout = "{{.Username}}"
	}

	if c.ShareFolder == "" {
		c.ShareFolder = "/MyShares"
	}

	// ensure share folder always starts with slash
	c.ShareFolder = path.Join("/", c.ShareFolder)

	c.DataDirectory = path.Join(c.Root, "data")
	c.Uploads = path.Join(c.Root, ".uploads")
	c.Shadow = path.Join(c.Root, ".shadow")

	c.References = path.Join(c.Shadow, "references")
	c.RecycleBin = path.Join(c.Shadow, "recycle_bin")
}

func (gl *Global) init(c *Config) {
	gl.SnapDir = ".snap"

	fi, _ := os.Stat(c.DataDirectory)
	stat, ok := fi.Sys().(*syscall.Stat_t)

	var dataInode uint64
	if ok {
		dataInode = stat.Ino
	}

	gl.SnapIno = strconv.FormatUint(dataInode, 10)
}

type cephfs struct {
	conf         *Config
	db           *sql.DB
	gl           *Global
	chunkHandler *chunking.ChunkHandler
}

// NewCephFS returns a storage.FS interface implementation that controls then
// ceph filesystem.
func NewCephFS(c *Config) (storage.FS, error) {
	c.init()

	// create namespaces if they do not exist
	namespaces := []string{c.DataDirectory, c.Uploads, c.Shadow, c.References, c.RecycleBin}
	for _, v := range namespaces {
		if err := os.MkdirAll(v, 0755); err != nil {
			return nil, errors.Wrap(err, "could not create home dir "+v)
		}
	}

	dbName := "cephfs.db"
	if !c.DisableHome {
		dbName = "cephhomefs.db"
	}

	db, err := initializeDB(c.Root, dbName)
	if err != nil {
		return nil, errors.Wrap(err, "cephfs: error initializing db")
	}

	gl := new(Global)
	gl.init(c)

	return &cephfs{
		conf:         c,
		db:           db,
		gl:           gl,
		chunkHandler: chunking.NewChunkHandler(c.Uploads),
	}, nil
}

func (fs *cephfs) Shutdown(ctx context.Context) error {
	err := fs.db.Close()
	if err != nil {
		return errors.Wrap(err, "cephfs: error closing db connection")
	}
	return nil
}

func (fs *cephfs) resolve(ctx context.Context, ref *provider.Reference) (string, error) {
	if ref.GetPath() != "" {
		return ref.GetPath(), nil
	}

	if ref.GetId() != nil {
		return fs.GetPathByID(ctx, ref.GetId())
	}

	// reference is invalid
	return "", fmt.Errorf("ceph: invalid reference %+v", ref)
}

func getUser(ctx context.Context) (*userpb.User, error) {
	u, ok := user.ContextGetUser(ctx)
	if !ok {
		err := errors.Wrap(errtypes.UserRequired(""), "ceph: error getting user from ctx")
		return nil, err
	}
	return u, nil
}

func (fs *cephfs) wrap(ctx context.Context, p string) string {
	var internal string
	if !fs.conf.DisableHome {
		layout, err := fs.GetHome(ctx)
		if err != nil {
			panic(err)
		}
		internal = path.Join(fs.conf.DataDirectory, layout, p)
	} else {
		internal = path.Join(fs.conf.DataDirectory, p)
	}
	return internal
}

func (fs *cephfs) wrapReferences(ctx context.Context, p string) string {
	var internal string
	if !fs.conf.DisableHome {
		layout, err := fs.GetHome(ctx)
		if err != nil {
			panic(err)
		}
		internal = path.Join(fs.conf.References, layout, p)
	} else {
		internal = path.Join(fs.conf.References, p)
	}
	return internal
}

func (fs *cephfs) wrapRecycleBin(ctx context.Context, p string) string {
	var internal string
	if !fs.conf.DisableHome {
		layout, err := fs.GetHome(ctx)
		if err != nil {
			panic(err)
		}
		internal = path.Join(fs.conf.RecycleBin, layout, p)
	} else {
		internal = path.Join(fs.conf.RecycleBin, p)
	}
	return internal
}

func (fs *cephfs) wrapVersions(ctx context.Context, p string) string {
	var internal string
	if !fs.conf.DisableHome {
		layout, err := fs.GetHome(ctx)
		if err != nil {
			panic(err)
		}
		internal = path.Join(layout, path.Dir(p), fs.gl.SnapDir)
	} else {
		internal = path.Join(path.Dir(p), fs.gl.SnapDir)
	}
	return internal
}

func (fs *cephfs) wrapSnap(key string) string {
	return "_" + key + "_" + fs.gl.SnapIno
}

func (fs *cephfs) unwrap(ctx context.Context, np string) string {
	ns := fs.getNsMatch(np, []string{fs.conf.DataDirectory, fs.conf.References, fs.conf.RecycleBin})
	var external string
	if !fs.conf.DisableHome {
		layout, err := fs.GetHome(ctx)
		if err != nil {
			panic(err)
		}
		ns = path.Join(ns, layout)
	}

	external = strings.TrimPrefix(np, ns)
	if external == "" {
		external = "/"
	}
	return external
}

func (fs *cephfs) getNsMatch(internal string, nss []string) string {
	var match string
	for _, ns := range nss {
		if strings.HasPrefix(internal, ns) && len(ns) > len(match) {
			match = ns
		}
	}
	if match == "" {
		panic(fmt.Sprintf("ceph: path is outside namespaces: path=%s namespaces=%+v", internal, nss))
	}

	return match
}

func (fs *cephfs) isShareFolder(ctx context.Context, p string) bool {
	return strings.HasPrefix(p, fs.conf.ShareFolder)
}

func (fs *cephfs) isShareFolderRoot(ctx context.Context, p string) bool {
	return path.Clean(p) == fs.conf.ShareFolder
}

func (fs *cephfs) isShareFolderChild(ctx context.Context, p string) bool {
	p = path.Clean(p)
	vals := strings.Split(p, fs.conf.ShareFolder+"/")
	return len(vals) > 1 && vals[1] != ""
}

func (fs *cephfs) normalize(ctx context.Context, fi os.FileInfo, fn string, mdKeys []string) (*provider.ResourceInfo, error) {
	fp := fs.unwrap(ctx, path.Join("/", fn))
	owner, err := getUser(ctx)
	if err != nil {
		return nil, err
	}
	metadata, err := fs.retrieveArbitraryMetadata(ctx, fn, mdKeys)
	if err != nil {
		return nil, err
	}

	var layout string
	if !fs.conf.DisableHome {
		layout, err = fs.GetHome(ctx)
		if err != nil {
			return nil, err
		}
	}

	// A fileid is constructed like `fileid-url_encoded_path`. See GetPathByID for the inverse conversion
	md := &provider.ResourceInfo{
		Id:            &provider.ResourceId{OpaqueId: "fileid-" + url.QueryEscape(path.Join(layout, fp))},
		Path:          fp,
		Type:          getResourceType(fi.IsDir()),
		Etag:          calcEtag(ctx, fi, fn),
		MimeType:      mime.Detect(fi.IsDir(), fp),
		Size:          uint64(fi.Size()),
		PermissionSet: &provider.ResourcePermissions{ListContainer: true, CreateContainer: true},
		Mtime: &types.Timestamp{
			Seconds: uint64(fi.ModTime().Unix()),
		},
		Owner:             owner.Id,
		ArbitraryMetadata: metadata,
	}

	return md, nil
}

func (fs *cephfs) convertToFileReference(ctx context.Context, fi os.FileInfo, fn string, mdKeys []string) (*provider.ResourceInfo, error) {
	info, err := fs.normalize(ctx, fi, fn, mdKeys)
	if err != nil {
		return nil, err
	}
	info.Type = provider.ResourceType_RESOURCE_TYPE_REFERENCE
	target, err := fs.getReferenceEntry(ctx, fn)
	if err != nil {
		return nil, err
	}
	info.Target = target
	return info, nil
}

func getResourceType(isDir bool) provider.ResourceType {
	if isDir {
		return provider.ResourceType_RESOURCE_TYPE_CONTAINER
	}
	return provider.ResourceType_RESOURCE_TYPE_FILE
}

func (fs *cephfs) retrieveArbitraryMetadata(ctx context.Context, fn string, mdKeys []string) (*provider.ArbitraryMetadata, error) {
	md, err := fs.getMetadata(ctx, fn)
	if err != nil {
		return nil, errors.Wrap(err, "cephfs: error listing metadata")
	}
	var mdKey, mdVal string
	metadata := map[string]string{}

	mdKeysMap := make(map[string]struct{})
	for _, k := range mdKeys {
		mdKeysMap[k] = struct{}{}
	}

	var returnAllKeys bool
	if _, ok := mdKeysMap["*"]; len(mdKeys) == 0 || ok {
		returnAllKeys = true
	}

	for md.Next() {
		err = md.Scan(&mdKey, &mdVal)
		if err != nil {
			return nil, errors.Wrap(err, "cephfs: error scanning db rows")
		}
		if _, ok := mdKeysMap[mdKey]; returnAllKeys || ok {
			metadata[mdKey] = mdVal
		}
	}
	return &provider.ArbitraryMetadata{
		Metadata: metadata,
	}, nil
}

// GetPathByID returns the path pointed by the file id
// In this implementation the file id is in the form `fileid-url_encoded_path`
func (fs *cephfs) GetPathByID(ctx context.Context, id *provider.ResourceId) (string, error) {
	var layout string
	if !fs.conf.DisableHome {
		var err error
		layout, err = fs.GetHome(ctx)
		if err != nil {
			return "", err
		}
	}
	return url.QueryUnescape(strings.TrimPrefix(id.OpaqueId, "fileid-"+layout))
}

func (fs *cephfs) AddGrant(ctx context.Context, ref *provider.Reference, g *provider.Grant) error {
	fn, err := fs.resolve(ctx, ref)
	if err != nil {
		return errors.Wrap(err, "cephfs: error resolving ref")
	}
	fn = fs.wrap(ctx, fn)

	role, err := grants.GetACLPerm(g.Permissions)
	if err != nil {
		return errors.Wrap(err, "cephfs: unknown set permissions")
	}

	granteeType, err := grants.GetACLType(g.Grantee.Type)
	if err != nil {
		return errors.Wrap(err, "cephfs: error getting grantee type")
	}
	grantee := fmt.Sprintf("%s:%s@%s", granteeType, g.Grantee.Id.OpaqueId, g.Grantee.Id.Idp)

	err = fs.addToACLDB(ctx, fn, grantee, role)
	if err != nil {
		return errors.Wrap(err, "cephfs: error adding entry to DB")
	}

	return fs.propagate(ctx, fn)
}

func (fs *cephfs) ListGrants(ctx context.Context, ref *provider.Reference) ([]*provider.Grant, error) {
	fn, err := fs.resolve(ctx, ref)
	if err != nil {
		return nil, errors.Wrap(err, "cephfs: error resolving ref")
	}
	fn = fs.wrap(ctx, fn)

	g, err := fs.getACLs(ctx, fn)
	if err != nil {
		return nil, errors.Wrap(err, "cephfs: error listing grants")
	}
	var granteeID, role string
	var grantList []*provider.Grant

	for g.Next() {
		err = g.Scan(&granteeID, &role)
		if err != nil {
			return nil, errors.Wrap(err, "cephfs: error scanning db rows")
		}
		grantee := &provider.Grantee{
			Id:   &userpb.UserId{OpaqueId: granteeID[2:]},
			Type: grants.GetGranteeType(string(granteeID[0])),
		}
		permissions := grants.GetGrantPermissionSet(role)

		grantList = append(grantList, &provider.Grant{
			Grantee:     grantee,
			Permissions: permissions,
		})
	}
	return grantList, nil

}

func (fs *cephfs) RemoveGrant(ctx context.Context, ref *provider.Reference, g *provider.Grant) error {
	fn, err := fs.resolve(ctx, ref)
	if err != nil {
		return errors.Wrap(err, "cephfs: error resolving ref")
	}
	fn = fs.wrap(ctx, fn)

	granteeType, err := grants.GetACLType(g.Grantee.Type)
	if err != nil {
		return errors.Wrap(err, "cephfs: error getting grantee type")
	}
	grantee := fmt.Sprintf("%s:%s@%s", granteeType, g.Grantee.Id.OpaqueId, g.Grantee.Id.Idp)

	err = fs.removeFromACLDB(ctx, fn, grantee)
	if err != nil {
		return errors.Wrap(err, "cephfs: error removing from DB")
	}

	return fs.propagate(ctx, fn)
}

func (fs *cephfs) UpdateGrant(ctx context.Context, ref *provider.Reference, g *provider.Grant) error {
	return fs.AddGrant(ctx, ref, g)
}

func (fs *cephfs) GetQuota(ctx context.Context) (int, int, error) {
	return 0, 0, nil
}

func (fs *cephfs) CreateReference(ctx context.Context, path string, targetURI *url.URL) error {
	if !fs.isShareFolder(ctx, path) {
		return errtypes.PermissionDenied("cephfs: cannot create references outside the share folder")
	}

	fn := fs.wrapReferences(ctx, path)

	err := os.MkdirAll(fn, 0700)
	if err != nil {
		if os.IsNotExist(err) {
			return errtypes.NotFound(fn)
		}
		return errors.Wrap(err, "cephfs: error creating dir "+fn)
	}

	if err = fs.addToReferencesDB(ctx, fn, targetURI.String()); err != nil {
		return errors.Wrap(err, "cephfs: error adding entry to DB")
	}

	return fs.propagate(ctx, fn)
}

func (fs *cephfs) SetArbitraryMetadata(ctx context.Context, ref *provider.Reference, md *provider.ArbitraryMetadata) error {

	np, err := fs.resolve(ctx, ref)
	if err != nil {
		return errors.Wrap(err, "cephfs: error resolving ref")
	}

	if fs.isShareFolderRoot(ctx, np) {
		return errtypes.PermissionDenied("cephfs: cannot set metadata for the virtual share folder")
	}

	if fs.isShareFolderChild(ctx, np) {
		np = fs.wrapReferences(ctx, np)
	} else {
		np = fs.wrap(ctx, np)
	}

	fi, err := os.Stat(np)
	if err != nil {
		if os.IsNotExist(err) {
			return errtypes.NotFound(fs.unwrap(ctx, np))
		}
		return errors.Wrap(err, "cephfs: error stating "+np)
	}

	if md.Metadata != nil {
		if val, ok := md.Metadata["mtime"]; ok {
			if mtime, err := parseMTime(val); err == nil {
				// updating mtime also updates atime
				if err := os.Chtimes(np, mtime, mtime); err != nil {
					return errors.Wrap(err, "could not set mtime")
				}
			} else {
				return errors.Wrap(err, "could not parse mtime")
			}
			delete(md.Metadata, "mtime")
		}

		if _, ok := md.Metadata["etag"]; ok {
			etag := calcEtag(ctx, fi, np)
			if etag != md.Metadata["etag"] {
				err = fs.addToMetadataDB(ctx, np, "etag", etag)
				if err != nil {
					return errors.Wrap(err, "cephfs: error adding entry to DB")
				}
			}
			delete(md.Metadata, "etag")
		}

		if _, ok := md.Metadata["favorite"]; ok {
			u, err := getUser(ctx)
			if err != nil {
				return err
			}
			if uid := u.GetId(); uid != nil {
				usr := fmt.Sprintf("u:%s@%s", uid.GetOpaqueId(), uid.GetIdp())
				if err = fs.addToFavoritesDB(ctx, np, usr); err != nil {
					return errors.Wrap(err, "cephfs: error adding entry to DB")
				}
			} else {
				return errors.Wrap(errtypes.UserRequired("userrequired"), "user has no id")
			}
			delete(md.Metadata, "favorite")
		}
	}

	for k, v := range md.Metadata {
		err = fs.addToMetadataDB(ctx, np, k, v)
		if err != nil {
			return errors.Wrap(err, "cephfs: error adding entry to DB")
		}
	}

	return fs.propagate(ctx, np)
}

func parseMTime(v string) (t time.Time, err error) {
	p := strings.SplitN(v, ".", 2)
	var sec, nsec int64
	if sec, err = strconv.ParseInt(p[0], 10, 64); err == nil {
		if len(p) > 1 {
			nsec, err = strconv.ParseInt(p[1], 10, 64)
		}
	}
	return time.Unix(sec, nsec), err
}

func (fs *cephfs) UnsetArbitraryMetadata(ctx context.Context, ref *provider.Reference, keys []string) error {

	np, err := fs.resolve(ctx, ref)
	if err != nil {
		return errors.Wrap(err, "cephfs: error resolving ref")
	}

	if fs.isShareFolderRoot(ctx, np) {
		return errtypes.PermissionDenied("cephfs: cannot set metadata for the virtual share folder")
	}

	if fs.isShareFolderChild(ctx, np) {
		np = fs.wrapReferences(ctx, np)
	} else {
		np = fs.wrap(ctx, np)
	}

	_, err = os.Stat(np)
	if err != nil {
		if os.IsNotExist(err) {
			return errtypes.NotFound(fs.unwrap(ctx, np))
		}
		return errors.Wrap(err, "cephfs: error stating "+np)
	}

	for _, k := range keys {
		switch k {
		case "favorite":
			u, err := getUser(ctx)
			if err != nil {
				return err
			}
			if uid := u.GetId(); uid != nil {
				usr := fmt.Sprintf("u:%s@%s", uid.GetOpaqueId(), uid.GetIdp())
				if err = fs.removeFromFavoritesDB(ctx, np, usr); err != nil {
					return errors.Wrap(err, "cephfs: error removing entry from DB")
				}
			} else {
				return errors.Wrap(errtypes.UserRequired("userrequired"), "user has no id")
			}
		case "etag":
			return errors.Wrap(errtypes.NotSupported("unsetting etag not supported"), "could not unset metadata")
		case "mtime":
			return errors.Wrap(errtypes.NotSupported("unsetting mtime not supported"), "could not unset metadata")
		default:
			err = fs.removeFromMetadataDB(ctx, np, k)
			if err != nil {
				return errors.Wrap(err, "cephfs: error adding entry to DB")
			}
		}
	}

	return fs.propagate(ctx, np)
}

func (fs *cephfs) GetHome(ctx context.Context) (string, error) {
	if fs.conf.DisableHome {
		return "", errtypes.NotSupported("ceph: get home not supported")
	}

	u, err := getUser(ctx)
	if err != nil {
		err = errors.Wrap(err, "ceph: wrap: no user in ctx and home is enabled")
		return "", err
	}
	relativeHome := templates.WithUser(u, fs.conf.UserLayout)

	return relativeHome, nil
}

func (fs *cephfs) CreateHome(ctx context.Context) error {
	if fs.conf.DisableHome {
		return errtypes.NotSupported("cephfs: create home not supported")
	}

	homePaths := []string{fs.wrap(ctx, "/"), fs.wrapRecycleBin(ctx, "/"), fs.wrapReferences(ctx, fs.conf.ShareFolder)}

	for _, v := range homePaths {
		fmt.Println(v)
		if err := fs.createHomeInternal(ctx, v); err != nil {
			return errors.Wrap(err, "ceph: error creating home dir "+v)
		}
	}

	return nil
}

func (fs *cephfs) createHomeInternal(ctx context.Context, fn string) error {
	_, err := os.Stat(fn)
	if err != nil {
		if !os.IsNotExist(err) {
			return errors.Wrap(err, "ceph: error stating:"+fn)
		}
	}
	err = os.MkdirAll(fn, 0700)
	if err != nil {
		return errors.Wrap(err, "ceph: error creating dir:"+fn)
	}
	return nil
}

func (fs *cephfs) CreateDir(ctx context.Context, fn string) error {

	if fs.isShareFolder(ctx, fn) {
		return errtypes.PermissionDenied("cephfs: cannot create folder under the share folder")
	}

	fn = fs.wrap(ctx, fn)
	if _, err := os.Stat(fn); err == nil {
		return errtypes.AlreadyExists(fn)
	}
	err := os.Mkdir(fn, 0700)
	if err != nil {
		if os.IsNotExist(err) {
			return errtypes.NotFound(fn)
		}
		return errors.Wrap(err, "cephfs: error creating dir "+fn)
	}
	return nil
}

func (fs *cephfs) Delete(ctx context.Context, ref *provider.Reference) error {
	fn, err := fs.resolve(ctx, ref)
	if err != nil {
		return errors.Wrap(err, "cephfs: error resolving ref")
	}

	if fs.isShareFolderRoot(ctx, fn) {
		return errtypes.PermissionDenied("cephfs: cannot delete the virtual share folder")
	}

	var fp string
	if fs.isShareFolderChild(ctx, fn) {
		fp = fs.wrapReferences(ctx, fn)
	} else {
		fp = fs.wrap(ctx, fn)
	}

	_, err = os.Stat(fp)
	if err != nil {
		if os.IsNotExist(err) {
			return errtypes.NotFound(fn)
		}
		return errors.Wrap(err, "cephfs: error stating "+fp)
	}

	key := fmt.Sprintf("%s.d%d", path.Base(fn), time.Now().UnixNano()/int64(time.Millisecond))
	if err := os.Rename(fp, fs.wrapRecycleBin(ctx, key)); err != nil {
		return errors.Wrap(err, "cephfs: could not delete item")
	}

	err = fs.addToRecycledDB(ctx, key, fn)
	if err != nil {
		return errors.Wrap(err, "cephfs: error adding entry to DB")
	}

	return fs.propagate(ctx, path.Dir(fp))
}

func (fs *cephfs) Move(ctx context.Context, oldRef, newRef *provider.Reference) error {
	oldName, err := fs.resolve(ctx, oldRef)
	if err != nil {
		return errors.Wrap(err, "cephfs: error resolving ref")
	}

	newName, err := fs.resolve(ctx, newRef)
	if err != nil {
		return errors.Wrap(err, "cephfs: error resolving ref")
	}

	if fs.isShareFolder(ctx, oldName) || fs.isShareFolder(ctx, newName) {
		return fs.moveReferences(ctx, oldName, newName)
	}

	oldName = fs.wrap(ctx, oldName)
	newName = fs.wrap(ctx, newName)

	if err := os.Rename(oldName, newName); err != nil {
		return errors.Wrap(err, "cephfs: error moving "+oldName+" to "+newName)
	}

	if err := fs.copyMD(oldName, newName); err != nil {
		return errors.Wrap(err, "cephfs: error copying metadata")
	}

	if err := fs.propagate(ctx, newName); err != nil {
		return err
	}
	if err := fs.propagate(ctx, path.Dir(oldName)); err != nil {
		return err
	}

	return nil
}

func (fs *cephfs) moveReferences(ctx context.Context, oldName, newName string) error {

	if fs.isShareFolderRoot(ctx, oldName) || fs.isShareFolderRoot(ctx, newName) {
		return errtypes.PermissionDenied("cephfs: cannot move/rename the virtual share folder")
	}

	// only rename of the reference is allowed, hence having the same basedir
	bold, _ := path.Split(oldName)
	bnew, _ := path.Split(newName)

	if bold != bnew {
		return errtypes.PermissionDenied("cephfs: cannot move references under the virtual share folder")
	}

	oldName = fs.wrapReferences(ctx, oldName)
	newName = fs.wrapReferences(ctx, newName)

	if err := os.Rename(oldName, newName); err != nil {
		return errors.Wrap(err, "cephfs: error moving "+oldName+" to "+newName)
	}

	if err := fs.copyMD(oldName, newName); err != nil {
		return errors.Wrap(err, "cephfs: error copying metadata")
	}

	if err := fs.propagate(ctx, newName); err != nil {
		return err
	}
	if err := fs.propagate(ctx, path.Dir(oldName)); err != nil {
		return err
	}

	return nil
}

func (fs *cephfs) GetMD(ctx context.Context, ref *provider.Reference, mdKeys []string) (*provider.ResourceInfo, error) {
	fn, err := fs.resolve(ctx, ref)
	if err != nil {
		return nil, errors.Wrap(err, "cephfs: error resolving ref")
	}

	if !fs.conf.DisableHome {
		if fs.isShareFolder(ctx, fn) {
			return fs.getMDShareFolder(ctx, fn, mdKeys)
		}
	}

	fn = fs.wrap(ctx, fn)
	md, err := os.Stat(fn)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errtypes.NotFound(fn)
		}
		return nil, errors.Wrap(err, "cephfs: error stating "+fn)
	}

	return fs.normalize(ctx, md, fn, mdKeys)
}

func (fs *cephfs) getMDShareFolder(ctx context.Context, p string, mdKeys []string) (*provider.ResourceInfo, error) {

	fn := fs.wrapReferences(ctx, p)
	md, err := os.Stat(fn)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errtypes.NotFound(fn)
		}
		return nil, errors.Wrap(err, "cephfs: error stating "+fn)
	}

	if fs.isShareFolderRoot(ctx, p) {
		return fs.normalize(ctx, md, fn, mdKeys)
	}
	return fs.convertToFileReference(ctx, md, fn, mdKeys)
}

func (fs *cephfs) ListFolder(ctx context.Context, ref *provider.Reference, mdKeys []string) ([]*provider.ResourceInfo, error) {
	fn, err := fs.resolve(ctx, ref)
	if err != nil {
		return nil, errors.Wrap(err, "cephfs: error resolving ref")
	}

	if fn == "/" {
		homeFiles, err := fs.listFolder(ctx, fn, mdKeys)
		if err != nil {
			return nil, err
		}
		if !fs.conf.DisableHome {
			sharedReferences, err := fs.listShareFolderRoot(ctx, fn, mdKeys)
			if err != nil {
				return nil, err
			}
			homeFiles = append(homeFiles, sharedReferences...)
		}
		return homeFiles, nil
	}

	if fs.isShareFolderRoot(ctx, fn) {
		return fs.listShareFolderRoot(ctx, fn, mdKeys)
	}

	if fs.isShareFolderChild(ctx, fn) {
		return nil, errtypes.PermissionDenied("cephfs: error listing folders inside the shared folder, only file references are stored inside")
	}

	return fs.listFolder(ctx, fn, mdKeys)
}

func (fs *cephfs) listFolder(ctx context.Context, fn string, mdKeys []string) ([]*provider.ResourceInfo, error) {

	fn = fs.wrap(ctx, fn)

	mds, err := ioutil.ReadDir(fn)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errtypes.NotFound(fn)
		}
		return nil, errors.Wrap(err, "cephfs: error listing "+fn)
	}

	finfos := []*provider.ResourceInfo{}
	for _, md := range mds {
		info, err := fs.normalize(ctx, md, path.Join(fn, md.Name()), mdKeys)
		if err == nil {
			finfos = append(finfos, info)
		}
	}
	return finfos, nil
}

func (fs *cephfs) listShareFolderRoot(ctx context.Context, home string, mdKeys []string) ([]*provider.ResourceInfo, error) {

	fn := fs.wrapReferences(ctx, home)

	mds, err := ioutil.ReadDir(fn)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errtypes.NotFound(fn)
		}
		return nil, errors.Wrap(err, "cephfs: error listing "+fn)
	}

	finfos := []*provider.ResourceInfo{}
	for _, md := range mds {
		var info *provider.ResourceInfo
		var err error
		if fs.isShareFolderRoot(ctx, path.Join("/", md.Name())) {
			info, err = fs.normalize(ctx, md, path.Join(fn, md.Name()), mdKeys)
		} else {
			info, err = fs.convertToFileReference(ctx, md, path.Join(fn, md.Name()), mdKeys)
		}
		if err == nil {
			finfos = append(finfos, info)
		}
	}
	return finfos, nil
}

func (fs *cephfs) Download(ctx context.Context, ref *provider.Reference) (io.ReadCloser, error) {
	fn, err := fs.resolve(ctx, ref)
	if err != nil {
		return nil, errors.Wrap(err, "cephfs: error resolving ref")
	}

	if fs.isShareFolder(ctx, fn) {
		return nil, errtypes.PermissionDenied("cephfs: cannot download under the virtual share folder")
	}

	fn = fs.wrap(ctx, fn)
	r, err := os.Open(fn)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errtypes.NotFound(fn)
		}
		return nil, errors.Wrap(err, "cephfs: error reading "+fn)
	}
	return r, nil
}

func (fs *cephfs) archiveRevision(ctx context.Context, np string) error {
	/*
		Either do nothing and let an external job to handle the snapshots
		Or create a snapshot of the file's directory, but that would be a total waste and it won't scale
	*/

	return nil
}

func (fs *cephfs) ListRevisions(ctx context.Context, ref *provider.Reference) ([]*provider.FileVersion, error) {
	np, err := fs.resolve(ctx, ref)
	if err != nil {
		return nil, errors.Wrap(err, "cephfs: error resolving ref")
	}

	if fs.isShareFolder(ctx, np) {
		return nil, errtypes.PermissionDenied("cephfs: cannot list revisions under the virtual share folder")
	}

	versionsDir := fs.wrapVersions(ctx, np)
	revisions := []*provider.FileVersion{}
	mds, err := ioutil.ReadDir(versionsDir)
	if err != nil {
		return nil, errors.Wrap(err, "cephfs: error reading"+versionsDir)
	}

	for i := range mds {
		// versions resemble _weekly2-<timestamp>_<data dir ino>
		version := strings.Split(mds[i].Name(), "_")[1]

		fileVersionPath := path.Join(versionsDir, mds[i].Name())
		file, err := os.Stat(fileVersionPath)
		if err != nil {
			return nil, errors.Wrap(err, "cephfs: error reading "+fileVersionPath)
		}

		//ignore weekly2 take the timestamp
		mtime, err := strconv.Atoi(strings.Split(version, "-")[1])
		if err != nil {
			continue
		}
		revisions = append(revisions, &provider.FileVersion{
			Key:   version,
			Size:  uint64(file.Size()),
			Mtime: uint64(mtime),
		})
	}
	return revisions, nil
}

func (fs *cephfs) DownloadRevision(ctx context.Context, ref *provider.Reference, revisionKey string) (io.ReadCloser, error) {
	normalPath, err := fs.resolve(ctx, ref)
	if err != nil {
		return nil, errors.Wrap(err, "cephfs: error resolving ref")
	}

	if fs.isShareFolder(ctx, normalPath) {
		return nil, errtypes.PermissionDenied("cephfs: cannot download revisions under the virtual share folder")
	}

	// path/to/file -> path/to/.snap/_version_SnapIno/file
	versionsDir := fs.wrapVersions(ctx, normalPath)
	versionPath := path.Join(versionsDir, fs.wrapSnap(revisionKey), path.Base(normalPath))

	r, err := os.Open(versionPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errtypes.NotFound(versionPath)
		}
		return nil, errors.Wrap(err, "cephfs: error reading "+versionPath)
	}

	return r, nil
}

func (fs *cephfs) RestoreRevision(ctx context.Context, ref *provider.Reference, revisionKey string) error {
	normalPath, err := fs.resolve(ctx, ref)
	if err != nil {
		return errors.Wrap(err, "cephfs: error resolving ref")
	}

	if fs.isShareFolder(ctx, normalPath) {
		return errtypes.PermissionDenied("cephfs: cannot restore revisions under the virtual share folder")
	}

	versionsDir := fs.wrapVersions(ctx, normalPath)
	versionPath := path.Join(versionsDir, fs.wrapSnap(revisionKey), path.Base(normalPath))
	normalPath = fs.wrap(ctx, normalPath)

	// check revision exists
	vs, err := os.Stat(versionPath)
	if err != nil {
		if os.IsNotExist(err) {
			return errtypes.NotFound(revisionKey)
		}
		return errors.Wrap(err, "cephfs: error stating "+versionPath)
	}

	if !vs.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", versionPath)
	}

	/*
		if err := fs.archiveRevision(ctx, normalPath); err != nil {
			return err
		}
	*/

	//snapshots are readonly, move doesn't work here, so we run cp to restore from a snapshot
	cmd := exec.Command("cp", versionPath, normalPath)
	if err := cmd.Run(); err != nil {
		return errors.Wrap(err, "cephfs: error renaming from "+versionPath+" to "+normalPath)
	}

	return fs.propagate(ctx, normalPath)
}

func (fs *cephfs) PurgeRecycleItem(ctx context.Context, key string) error {
	rp := fs.wrapRecycleBin(ctx, key)

	if err := os.Remove(rp); err != nil {
		return errors.Wrap(err, "cephfs: error deleting recycle item")
	}
	return nil
}

func (fs *cephfs) EmptyRecycle(ctx context.Context) error {
	rp := fs.wrapRecycleBin(ctx, "/")

	if err := os.RemoveAll(rp); err != nil {
		return errors.Wrap(err, "cephfs: error deleting recycle files")
	}
	if err := fs.createHomeInternal(ctx, rp); err != nil {
		return errors.Wrap(err, "cephfs: error deleting recycle files")
	}
	return nil
}

func (fs *cephfs) convertToRecycleItem(ctx context.Context, rp string, md os.FileInfo) *provider.RecycleItem {
	// trashbin items have filename.txt.d12345678
	suffix := path.Ext(md.Name())
	if len(suffix) == 0 || !strings.HasPrefix(suffix, ".d") {
		return nil
	}

	trashtime := suffix[2:]
	ttime, err := strconv.Atoi(trashtime)
	if err != nil {
		return nil
	}

	filePath, err := fs.getRecycledEntry(ctx, md.Name())
	if err != nil {
		return nil
	}

	return &provider.RecycleItem{
		Type: getResourceType(md.IsDir()),
		Key:  md.Name(),
		Path: filePath,
		Size: uint64(md.Size()),
		DeletionTime: &types.Timestamp{
			Seconds: uint64(ttime),
		},
	}
}

func (fs *cephfs) ListRecycle(ctx context.Context) ([]*provider.RecycleItem, error) {
	rp := fs.wrapRecycleBin(ctx, "/")

	mds, err := ioutil.ReadDir(rp)
	if err != nil {
		return nil, errors.Wrap(err, "cephfs: error listing deleted files")
	}
	items := []*provider.RecycleItem{}
	for i := range mds {
		ri := fs.convertToRecycleItem(ctx, rp, mds[i])
		if ri != nil {
			items = append(items, ri)
		}
	}
	return items, nil
}

func (fs *cephfs) RestoreRecycleItem(ctx context.Context, restoreKey string) error {
	suffix := path.Ext(restoreKey)
	if len(suffix) == 0 || !strings.HasPrefix(suffix, ".d") {
		return errors.New("cephfs: invalid trash item suffix")
	}

	filePath, err := fs.getRecycledEntry(ctx, restoreKey)
	if err != nil {
		return errors.Wrap(err, "cephfs: invalid key")
	}

	var originalPath string
	if fs.isShareFolder(ctx, filePath) {
		originalPath = fs.wrapReferences(ctx, filePath)
	} else {
		originalPath = fs.wrap(ctx, filePath)
	}

	if _, err = os.Stat(originalPath); err == nil {
		return errors.New("cephfs: can't restore - file already exists at original path")
	}

	rp := fs.wrapRecycleBin(ctx, restoreKey)
	if _, err = os.Stat(rp); err != nil {
		if os.IsNotExist(err) {
			return errtypes.NotFound(restoreKey)
		}
		return errors.Wrap(err, "cephfs: error stating "+rp)
	}

	if err := os.Rename(rp, originalPath); err != nil {
		return errors.Wrap(err, "ocfs: could not restore item")
	}

	err = fs.removeFromRecycledDB(ctx, restoreKey)
	if err != nil {
		return errors.Wrap(err, "cephfs: error adding entry to DB")
	}

	return fs.propagate(ctx, originalPath)
}

func (fs *cephfs) propagate(ctx context.Context, leafPath string) error {
	var root string
	if fs.isShareFolderChild(ctx, leafPath) {
		root = fs.wrapReferences(ctx, "/")
	} else {
		root = fs.wrap(ctx, "/")
	}

	if !strings.HasPrefix(leafPath, root) {
		return errors.New("internal path outside root")
	}

	fi, err := os.Stat(leafPath)
	if err != nil {
		return err
	}

	parts := strings.Split(strings.TrimPrefix(leafPath, root), "/")
	// root never ends in / so the split returns an empty first element, which we can skip
	// we do not need to chmod the last element because it is the leaf path (< and not <= comparison)
	for i := 1; i < len(parts); i++ {
		if err := os.Chtimes(root, fi.ModTime(), fi.ModTime()); err != nil {
			return err
		}
		root = path.Join(root, parts[i])
	}
	return nil
}
