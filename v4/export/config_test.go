// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"testing"

	tcontext "github.com/pingcap/dumpling/v4/context"
	"github.com/stretchr/testify/require"
)

func TestCreateExternalStorage(t *testing.T) {
	t.Parallel()
	mockConfig := defaultConfigForTest(t)
	loc, err := mockConfig.createExternalStorage(tcontext.Background())
	require.NoError(t, err)
	require.Regexp(t, "file:.*", loc.URI())
}
