// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package export

import (
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
)

const (
	dumpChunkRetryTime       = 3
	dumpChunkWaitInterval    = 50 * time.Millisecond
	dumpChunkMaxWaitInterval = 200 * time.Millisecond
)

func newDumpChunkBackoffer(canRetry bool) *dumpChunkBackoffer {
	if !canRetry {
		return &dumpChunkBackoffer{
			attempt: 1,
		}
	}
	return &dumpChunkBackoffer{
		attempt:      dumpChunkRetryTime,
		delayTime:    dumpChunkWaitInterval,
		maxDelayTime: dumpChunkMaxWaitInterval,
	}
}

type dumpChunkBackoffer struct {
	attempt      int
	delayTime    time.Duration
	maxDelayTime time.Duration
}

func (b *dumpChunkBackoffer) NextBackoff(err error) time.Duration {
	err = errors.Cause(err)
	if _, ok := err.(*mysql.MySQLError); ok && !dbutil.IsRetryableError(err) {
		b.attempt = 0
		return 0
	} else if _, ok := err.(*writerError); ok {
		// the uploader writer's retry logic is already done in aws client. needn't retry here
		b.attempt = 0
		return 0
	}
	b.delayTime = 2 * b.delayTime
	b.attempt--
	if b.delayTime > b.maxDelayTime {
		return b.maxDelayTime
	}
	return b.delayTime
}

func (b *dumpChunkBackoffer) Attempt() int {
	return b.attempt
}
