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
	cephfs2 "github.com/ceph/go-ceph/cephfs"
)

func NewCephMount() (*cephfs2.MountInfo, error) {
	mount, err := cephfs2.CreateMount()
	if err = mount.ReadDefaultConfigFile(); err != nil { return nil, err }
	if err = mount.Mount(); err != nil { return nil, err }

	return mount, err
}
