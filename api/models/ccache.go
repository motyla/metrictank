package models

import (
	opentracing "github.com/opentracing/opentracing-go"
)

type CCacheDelete struct {
	Patterns  []string `json:"patterns" form:"patterns" binding:"Required"`
	OrgId     int      `json:"orgId" form:"orgId" binding:"Required"`
	Propagate bool     `json:"propagate" form:"propagate"`
}

func (cd CCacheDelete) Trace(span opentracing.Span) {
	span.SetTag("patterns", cd.Patterns)
	span.SetTag("org", cd.OrgId)
	span.SetTag("propagate", cd.Propagate)
}

type CCacheDeleteResp struct {
	Errors          int                         `json:"errors"`
	DeletedSeries   int                         `json:"deletedSeries"`
	DeletedArchives int                         `json:"deletedArchives"`
	Peers           map[string]CCacheDeleteResp `json:"peers"`
}
