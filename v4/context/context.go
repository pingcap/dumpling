// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.
// This code is copied from https://github.com/pingcap/dm/blob/master/pkg/context/context.go

package context

import (
	"context"
	"time"

	"github.com/pingcap/dumpling/v4/log"
)

// Context is used to in dm to record some context field like
// * go context
// * logger
type Context struct {
	ctx    context.Context
	logger log.Logger
}

// Background return a nop context
func Background() *Context {
	return &Context{
		ctx:    context.Background(),
		logger: log.Zap(),
	}
}

// NewContext return a new Context
func NewContext(ctx context.Context, logger log.Logger) *Context {
	return &Context{
		ctx:    ctx,
		logger: logger,
	}
}

// WithContext set go context
func (c *Context) WithContext(ctx context.Context) *Context {
	return &Context{
		ctx:    ctx,
		logger: c.logger,
	}
}

// WithTimeout sets a timeout associated context.
func (c *Context) WithTimeout(timeout time.Duration) (*Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(c.ctx, timeout)
	return &Context{
		ctx:    ctx,
		logger: c.logger,
	}, cancel
}

// Context returns real context
func (c *Context) Context() context.Context {
	return c.ctx
}

// WithLogger set logger
func (c *Context) WithLogger(logger log.Logger) *Context {
	return &Context{
		ctx:    c.ctx,
		logger: logger,
	}
}

// L returns real logger
func (c *Context) L() log.Logger {
	return c.logger
}
