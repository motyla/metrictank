package cluster

import (
	"context"
)

type NodeIf interface {
	IsLocal() bool
	IsReady() bool
	GetPartitions() []int32
	GetPriority() int
	Post(context.Context, string, string, Traceable) ([]byte, error)
	GetName() string
}
