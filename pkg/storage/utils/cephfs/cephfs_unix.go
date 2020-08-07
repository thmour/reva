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
	"fmt"
	"os"
	"strings"
	"syscall"

	"github.com/pkg/xattr"

	"github.com/cs3org/reva/pkg/appctx"
)

// calcEtag will create an etag based on the md5 of
// - mtime,
// - inode (if available),
// - device (if available) and
// - size.
// errors are logged, but an etag will still be returned
func calcEtag(ctx context.Context, fi os.FileInfo, path string) string {
	var str string
	log := appctx.GetLogger(ctx)
	stat, ok := fi.Sys().(*syscall.Stat_t)

	if ok {
		if fi.IsDir() {
			rctime, err := xattr.Get(path, "ceph.dir.rctime")
			if err != nil {
				log.Error().Err(err).Msg("error writing rctime")
			}
			str = fmt.Sprintf("\"%s%d\"", strings.Replace(string(rctime), ".", "", -1), stat.Ino)
		} else {
			str = fmt.Sprintf("\"%d%d:%d\"", stat.Ctim.Sec, stat.Ctim.Nsec, stat.Ino)
		}
	} else {
		log.Error().Msg("error stat file/dir")
	}
	return str
}
