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
	Ctx    context.Context
	Logger log.Logger
}

// Background return a nop context
func Background() *Context {
	return &Context{
		Ctx:    context.Background(),
		Logger: log.Zap(),
	}
}

// NewContext return a new Context
func NewContext(ctx context.Context, logger log.Logger) *Context {
	return &Context{
		Ctx:    ctx,
		Logger: logger,
	}
}

// WithContext set go context
func (c *Context) WithContext(ctx context.Context) *Context {
	return &Context{
		Ctx:    ctx,
		Logger: c.Logger,
	}
}

// WithTimeout sets a timeout associated context.
func (c *Context) WithTimeout(timeout time.Duration) (*Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(c.Ctx, timeout)
	return &Context{
		Ctx:    ctx,
		Logger: c.Logger,
	}, cancel
}

// Context returns real context
func (c *Context) Context() context.Context {
	return c.Ctx
}

// WithLogger set logger
func (c *Context) WithLogger(logger log.Logger) *Context {
	return &Context{
		Ctx:    c.Ctx,
		Logger: logger,
	}
}

// L returns real logger
func (c *Context) L() log.Logger {
	return c.Logger
}
