package carbonserver

import (
	"errors"
	"math"
	_ "net/http/pprof"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/lomik/go-carbon/points"
)

// https://github.com/golang/go/issues/448
func mod(a, b int) int {
	m := a % b
	if m < 0 {
		m += b
	}
	return m
}

// from go-graphite/go-whisper
func interval(time int, secondsPerPoint int) int {
	return time - mod(time, secondsPerPoint) + secondsPerPoint
}

func (listener *CarbonserverListener) fetchFromCache(metric string, fromTime, untilTime int32, resp *response) ([]points.Point, error) {
	var step int32
	path := listener.whisperData + "/" + strings.Replace(metric, ".", "/", -1) + ".wsp"

	logger := listener.logger.With(
		zap.String("path", path),
		zap.Int("fromTime", int(fromTime)),
		zap.Int("untilTime", int(untilTime)),
	)

	// query cache
	cacheStartTime := time.Now()
	cacheData := listener.cacheGet(metric)
	waitTime := time.Since(cacheStartTime)
	atomic.AddUint64(&listener.metrics.CacheWaitTimeFetchNS, uint64(waitTime.Nanoseconds()))
	listener.prometheus.cacheDuration("wait", waitTime)

	if cacheData == nil {
		// we really can't find this metric
		atomic.AddUint64(&listener.metrics.NotFound, 1)
		listener.logger.Error("No cache data error", zap.String("path", path))
		return nil, errors.New("No cache data error")
	}

	logger.Debug("fetching cache only metric")

	// retentions, aggMethod, xFilesFactor from matched schema/agg
	schema, aggr := listener.persisterMatch(metric)
	if schema == nil {
		logger.Warn("no storage schema defined for metric", zap.String("metric", metric))
		return nil, errors.New("no storage schema defined")
	}
	if aggr == nil {
		logger.Warn("no storage aggregation defined for metric", zap.String("metric", metric))
		return nil, errors.New("no storage aggregation defined")
	}

	retentions := schema.ResolveRetentions()

	step = int32(retentions[0].SecondsPerPoint())

	var _vals []float64
	for i := 0; i < int((untilTime-fromTime)/step); i++ {
		_vals = append(_vals, math.NaN())
	}

	resp.StartTime = int64(interval(int(fromTime), int(step)))
	resp.StopTime = int64(interval(int(untilTime), int(step)))
	resp.StepTime = int64(int(step))
	resp.Values = _vals
	resp.ConsolidationFunc = aggr.Name()
	resp.XFilesFactor = float32(aggr.XFilesFactor())

	atomic.AddUint64(&listener.metrics.MetricsReturned, 1)
	listener.prometheus.returnedMetric()

	atomic.AddUint64(&listener.metrics.PointsReturned, uint64(len(_vals)))
	listener.prometheus.returnedPoint(len(_vals))

	return cacheData, nil
}
