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
	"errors"
	cephfs2 "github.com/ceph/go-ceph/cephfs"
	userv1beta1 "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	"github.com/dgraph-io/ristretto"
	"time"
)

type CephClientPool struct {
	cache *ristretto.Cache
	maxConns int64
	ttl time.Duration
	admMount *cephfs2.MountInfo
}

type CephMount struct {
	mount *cephfs2.MountInfo
}

func NewCephClientPool() (*CephClientPool, error) {
	var maxItems int64 = 1000
	ttl, _ := time.ParseDuration("30m")
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 10*maxItems,
		MaxCost: maxItems,
		BufferItems: 64,
		OnEvict: func(key, conflict uint64, value interface{}, cost int64) {
			cm := value.(*CephMount)
			cm.mount.Release()
		},
	})

	mount, _ := cephfs2.CreateMount()
	if err = mount.ReadDefaultConfigFile(); err != nil { return nil, err }
	if err = mount.SetConfigOption("name", "admin"); err != nil { return nil, err }
	if err = mount.Mount(); err != nil { return nil, err }

	return &CephClientPool{cache: cache, maxConns: maxItems, ttl: ttl, admMount: mount}, err
}

func (p *CephClientPool) get(user *userv1beta1.User) (*cephfs2.MountInfo, error) {
	var mount *cephfs2.MountInfo
	var err error
	var value, found = p.cache.Get(user)
	if !found {
		if mount, err = cephfs2.CreateMount(); err != nil { goto _error }
		if err = mount.ReadDefaultConfigFile(); err != nil { goto _error }
		if err = mount.SetConfigOption("name", user.Username); err != nil { goto _error }
		if err = mount.Mount(); err != nil { goto _error }

		p.cache.SetWithTTL(user, mount, 1, p.ttl)
		if value, found = p.cache.Get(user); !found { err = errors.New("not found"); goto _error }

		return value.(*cephfs2.MountInfo), nil
		_error:
			return nil, err
	}

	return value.(*cephfs2.MountInfo), nil
}