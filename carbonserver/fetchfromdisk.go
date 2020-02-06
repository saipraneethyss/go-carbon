package carbonserver

import (
	"errors"
    "math"
	_ "net/http/pprof"
    "os"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/go-graphite/go-whisper"
	pnts "github.com/lomik/go-carbon/points"
)

type Metadata struct {
	ConsolidationFunc string
	XFilesFactor      float32
}

// recreated the go-graphite/go-whisper TimeSeries
// here because the properties are not public
// and no constructor/setters are implemented
type TimeSeries struct {
    fromTime  int
    untilTime int
    step      int
    values    []float64
}

func (ts *TimeSeries) FromTime() int {
    return ts.fromTime
}

func (ts *TimeSeries) UntilTime() int {
    return ts.untilTime
}

func (ts *TimeSeries) Step() int {
    return ts.step
}

func (ts *TimeSeries) Values() []float64 {
    return ts.values
}

type metricFromDisk struct {
	DiskStartTime time.Time
	CacheData     []pnts.Point
	Timeseries    *TimeSeries
	Metadata      Metadata
}

//Implementation of modulo that works like Python
//from go-graphite/go-whisper, credited there to @timmow
func mod(a, b int) int {
	return a - (b * int(math.Floor(float64(a)/float64(b))))
}

// also from go-graphite/go-whisper
func interval(time int, secondsPerPoint int) int {
	return time - mod(time, secondsPerPoint) + secondsPerPoint
}

func (listener *CarbonserverListener) fetchFromDisk(metric string, fromTime, untilTime int32) (*metricFromDisk, error) {
	var step int32
    var retentions []whisper.Retention
    var aggMethod string
    var xFilesFactor float32
    var points *TimeSeries
    skipCache := false

	path := listener.whisperData + "/" + strings.Replace(metric, ".", "/", -1) + ".wsp"

	logger := listener.logger.With(
		zap.String("path", path),
		zap.Int("fromTime", int(fromTime)),
		zap.Int("untilTime", int(untilTime)),
	)

	// we try to open the whisper file
	w, werr := whisper.OpenWithOptions(path, &whisper.Options{
		FLock: listener.flock,
	})
    if werr == nil {
        // if success, we will retrieve the metadata
        retentions = w.Retentions()
        aggMethod = w.AggregationMethod()
        xFilesFactor = w.XFilesFactor()
    } else if os.IsNotExist(werr) {
        // expandGlobs() should filter out non-existant metrics
        // so if we get here we are querying on cache-only metrics
        schema, aggr := listener.persisterMatch(metric)
        if schema == nil {
            logger.Warn("no storage schema defined for metric", zap.String("metric", metric))
            return nil, errors.New("no storage schema defined")
        }
        if aggr == nil {
            logger.Warn("no storage aggregation defined for metric", zap.String("metric", metric))
            return nil, errors.New("no storage aggregation defined")
        }

        // retentions, aggMethod, xFilesFactor from matched schema/agg
        // points a default Timeseries instance with no data
        retentions = schema.ResolveRetentions()
        aggMethod = aggr.Name()
        xFilesFactor = float32(aggr.XFilesFactor())
    } else {
        // expandGlobs() should filter out requests for
        // metrics we don't have, but just in case
        atomic.AddUint64(&listener.metrics.NotFound, 1)
        listener.logger.Error("open error", zap.String("path", path), zap.Error(werr))
        return nil, werr
    }

PROCESS:
	now := int32(time.Now().Unix())
	diff := now - fromTime
	bestStep := int32(retentions[0].SecondsPerPoint())
	for _, retention := range retentions {
		if int32(retention.MaxRetention()) >= diff {
			step = int32(retention.SecondsPerPoint())
			break
		}
	}

	if step == 0 {
		maxRetention := int32(retentions[len(retentions)-1].MaxRetention())
		if now-maxRetention > untilTime {
			logger.Warn("can't find proper archive for the request")
			return nil, errors.New("Can't find proper archive")
		}
		logger.Debug("can't find archive that contains full set of data, using the least precise one")
		step = maxRetention
	}

	res := &metricFromDisk{
		Metadata: Metadata{
			ConsolidationFunc: aggMethod,
			XFilesFactor:      xFilesFactor,
		},
	}
	if step != bestStep {
		logger.Debug("cache is not supported for this query (required step != best step)",
			zap.Int("step", int(step)),
			zap.Int("bestStep", int(bestStep)),
		)
    } else if skipCache {
		logger.Debug("cache is not supported for this query (this is a retry query)")
    } else {
		// query cache
		cacheStartTime := time.Now()
		res.CacheData = listener.cacheGet(metric)
		waitTime := time.Since(cacheStartTime)
		atomic.AddUint64(&listener.metrics.CacheWaitTimeFetchNS, uint64(waitTime.Nanoseconds()))
		listener.prometheus.cacheDuration("wait", waitTime)
	}

    if res.CacheData == nil && werr != nil && os.IsNotExist(werr) {
		logger.Debug("cache and filesystem both returned nothing; retry query")
        // we didn't find a metric on disk and we tried reading the cache
        // but got no data there either, so perhaps the metric was written
        // when we were busy and we should check again
        w, werr := whisper.OpenWithOptions(path, &whisper.Options{
            FLock: listener.flock,
        })
        if werr == nil {
            logger.Debug("retry query successful; reprocessing")
            // if success, we will retrieve the metadata
            retentions = w.Retentions()
            aggMethod = w.AggregationMethod()
            xFilesFactor = w.XFilesFactor()
            // and now reprocess, without cache
            skipCache = true
            goto PROCESS
        } else {
            // we really can't find this metric
            atomic.AddUint64(&listener.metrics.NotFound, 1)
            listener.logger.Error("open error", zap.String("path", path), zap.Error(werr))
            return nil, werr
        }
    }

    if werr == nil {
        res.DiskStartTime = time.Now()
        logger.Debug("fetching disk metric")
        atomic.AddUint64(&listener.metrics.DiskRequests, 1)
        listener.prometheus.diskRequest()

        _points, err := w.Fetch(int(fromTime), int(untilTime))
        w.Close()
        if err != nil {
            logger.Warn("failed to fetch points", zap.Error(err))
            return nil, errors.New("failed to fetch points")
        }

        // Should never happen, because we have a check for proper archive now
        if _points == nil {
            logger.Warn("metric time range not found")
            return nil, errors.New("time range not found")
        }

        points = &TimeSeries{
            fromTime:  _points.FromTime(),
            untilTime: _points.UntilTime(),
            step:      _points.Step(),
            values:    _points.Values(),
        }

        waitTime := time.Since(res.DiskStartTime)
        atomic.AddUint64(&listener.metrics.DiskWaitTimeNS, uint64(waitTime.Nanoseconds()))
        listener.prometheus.diskWaitDuration(waitTime)
    } else {
        var _vals []float64
        for i := 0; i < int((untilTime - fromTime) / step); i++ {
            _vals = append(_vals, math.NaN())
        }
        points = &TimeSeries{
            fromTime:  interval(int(fromTime), int(step)),
            untilTime: interval(int(untilTime), int(step)),
            step:      int(step),
            values:    _vals,
        }
    }

	atomic.AddUint64(&listener.metrics.MetricsReturned, 1)
	listener.prometheus.returnedMetric()

	values := points.Values()
	atomic.AddUint64(&listener.metrics.PointsReturned, uint64(len(values)))
	listener.prometheus.returnedPoint(len(values))

	res.Timeseries = points

	return res, nil
}
