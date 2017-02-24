package api

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"

	"github.com/raintank/metrictank/api/middleware"
	"github.com/raintank/metrictank/api/models"
	"github.com/raintank/metrictank/api/response"
	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/worldping-api/pkg/log"
)

func (s *Server) ccacheDelete(ctx *middleware.Context, req models.CCacheDelete) {
	res := models.CCacheDeleteResp{}

	if req.Propagate {
		res.Peers = s.ccacheDeletePropagate(ctx.Req.Context(), &req)
	}

	for _, pattern := range req.Patterns {
		nodes, err := s.MetricIndex.Find(req.OrgId, pattern, 0)
		if err != nil {
			response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
			return
		}
		for _, node := range nodes {
			for _, def := range node.Defs {
				delResult := s.Cache.DelMetric(def.Id)
				res.DeletedSeries += delResult.Series
				res.DeletedArchives += delResult.Archives
			}
		}
	}
	response.Write(ctx, response.NewJson(200, res, ""))
}

func (s *Server) ccacheDeletePropagate(ctx context.Context, req *models.CCacheDelete) map[string]models.CCacheDeleteResp {
	// we never want to propagate more than once to avoid loops
	req.Propagate = false

	peers := cluster.Manager.MemberList()
	peerResults := make(map[string]models.CCacheDeleteResp)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, peer := range peers {
		if peer.IsLocal() {
			continue
		}
		wg.Add(1)
		go func(peer cluster.NodeIf) {
			mu.Lock()
			defer mu.Unlock()
			peerResults[peer.GetName()] = s.ccacheDeleteRemote(ctx, req, peer)
			wg.Done()
		}(peer)
	}
	wg.Wait()

	return peerResults
}

func (s *Server) ccacheDeleteRemote(ctx context.Context, req *models.CCacheDelete, peer cluster.NodeIf) models.CCacheDeleteResp {
	var res models.CCacheDeleteResp

	log.Debug("HTTP metricDelete calling %s/ccache/delete", peer.GetName())
	buf, err := peer.Post(ctx, "ccacheDeleteRemote", "/ccache/delete", *req)
	if err != nil {
		log.Error(4, "HTTP ccacheDelete error querying %s/ccache/delete: %q", peer.GetName(), err)
		res.Errors++
		return res
	}

	err = json.Unmarshal(buf, &res)
	if err != nil {
		log.Error(4, "HTTP ccacheDelete error unmarshaling body from %s/ccache/delete: %q", peer.GetName(), err)
		res.Errors++
		return res
	}

	return res
}
