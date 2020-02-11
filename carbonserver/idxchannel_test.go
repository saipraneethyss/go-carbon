// +build skipchan

package carbonserver

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"
	// "strings"
	"github.com/lomik/go-carbon/cache"
	"github.com/lomik/go-carbon/persister"
	"github.com/lomik/go-carbon/points"
	"go.uber.org/zap"
)

var day = 60 * 60 * 24
var now = (int(time.Now().Unix()) / 120) * 120

var addMetrics = [...]string{
	"new.data.point1",
	"same.data.new.point1",
	"same.data.new.point2",
	"totally.new.point",
	"fresh.add",
}

var addFiles = [...]string{
	"path/to/file/name1.wsp",
	"path/to/file/name2.wsp",
	"path/to/file1/name1.wsp",
	"file/name1.wsp",
	"justname.wsp",
}

var removeFiles = [...]string{
	"path/to/file/name2.wsp",
	"path/to/file1/name1.wsp",
	"justname.wsp",
}

type TestCacheListener struct {
	idxUpdateChan chan MetricUpdate
}

func (c TestCacheListener) OnAdd(metricName string) {
	newMetricUpt := MetricUpdate{metricName, ADD}
	fmt.Println("******=====****** ADD operation in CACHE ;sending this info to channel - ", metricName)
	select {
	case c.idxUpdateChan <- newMetricUpt:
	default:
		fmt.Println("******=====****** index update channel is full in cache")
		panic(fmt.Sprintf("index update channel is full, dropping this metric - %v", newMetricUpt.Name))
	}
}

func (c TestCacheListener) OnDelete(metricName string) {
	newMetricUpt := MetricUpdate{metricName, DEL}
	fmt.Println("******=====****** DEL operation in CACHE ;sending this info to channel - ", metricName)
	select {
	case c.idxUpdateChan <- newMetricUpt:
	default:
		fmt.Println("******=====****** index update channel is full in cache")
		panic(fmt.Sprintf("index update channel is full, dropping this metric - %v", newMetricUpt.Name))
	}
}

type testInfo struct {
	forceChan     chan struct{}
	exitChan      chan struct{}
	testCache     *cache.Cache
	csListener    *CarbonserverListener
	scanFrequency <-chan time.Time
}

func getMetricRetentionAggregation(name string) (schema *persister.Schema, aggr *persister.WhisperAggregationItem) {
	retentionStr := "60s:90d"
	pattern, _ := regexp.Compile(".*")
	retentions, _ := persister.ParseRetentionDefs(retentionStr)
	f := false
	schema = &persister.Schema{
		Name:         "test",
		Pattern:      pattern,
		RetentionStr: retentionStr,
		Retentions:   retentions,
		Priority:     10,
		Compressed:   &f,
	}
	aggr = persister.NewWhisperAggregation().Match(name)
	return
}

func addFileToSys(file string, tmpDir string) error {
	path := filepath.Dir(file)
	if err := addFilePathToDir(path, tmpDir); err != nil {
		return err
	}
	if nfile, err := os.OpenFile(filepath.Join(tmpDir, file), os.O_RDONLY|os.O_CREATE, 0644); err == nil {
		nfile.Close()
		return nil
	} else {
		return nil
	}
}

func addFilePathToDir(filePath string, tmpDir string) error {
	err := os.MkdirAll(filepath.Join(tmpDir, filePath), 0755)
	if err != nil {
		os.RemoveAll(tmpDir)
	}
	return err
}

func removeFileFromDir(filePath string, tmpDir string) error {
	return os.Remove(filepath.Join(tmpDir, filePath))
}

func getTestDir() (string, error) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		return "", fmt.Errorf("error creating temp directory: %v\n", err)
	}
	return tmpDir, nil
}

func getTestInfo(dir string) *testInfo {
	// scanTime := 3 * time.Second
	// indexUptChan := make(chan metrics.MetricUpdate, 20)
	c := cache.New()
	carbonserver := NewCarbonserverListener(c.Get, getMetricRetentionAggregation)
	carbonserver.whisperData = dir
	carbonserver.logger = zap.NewNop()
	carbonserver.metrics = &metricStruct{}
	carbonserver.exitChan = make(chan struct{})
	// carbonserver.trieIndex = true
	carbonserver.trigramIndex = true
	cacheListener := TestCacheListener{carbonserver.IdxUptChan}
	c.SetCacheEventListener(cacheListener)
	// c.SetIdxUptChan(carbonserver.IdxUptChan)

	return &testInfo{
		forceChan:     make(chan struct{}),
		exitChan:      make(chan struct{}),
		scanFrequency: time.Tick(3 * time.Second),
		testCache:     c,
		csListener:    carbonserver,
	}
}

func (f *testInfo) checkExpandGlobs(t *testing.T, query string, shdExist bool) {
	fmt.Println("the query is - ", query)
	expandedGlobs, err := f.csListener.getExpandedGlobs(context.TODO(), zap.NewNop(), time.Now(), []string{query})
	if err != nil {
		t.Errorf("Unexpected err: '%v', expected: 'nil'", err)
		return
	}

	if expandedGlobs == nil {
		t.Errorf("No globs returned")
		return
	}

	fmt.Println("************* the expanded globs - ", expandedGlobs)

	if shdExist {
		file := expandedGlobs[0].Files[0]
		if file != query {
			t.Errorf("files: '%v', epxected: '%s'\n", file, query)
			return
		}
	} else {
		if len(expandedGlobs[0].Files) != 0 {
			t.Errorf("expected no files but found - '%v'\n", expandedGlobs[0].Files)
			return
		}
	}
}

func TestIndexUpdateOverChannel(t *testing.T) {
	tmpDir, err := getTestDir()
	if err != nil {
		fmt.Printf("unable to create test dir tree: %v\n", err)
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	for _, filePath := range addFiles {
		if err = addFileToSys(filePath, tmpDir); err != nil {
			fmt.Errorf("error creating temp directory for - %s\n error is: %v\n", filePath, err)
			t.Fatal(err)
		}
	}

	// get test info
	f := getTestInfo(tmpDir)

	//start filewalk
	go f.csListener.updateMetricsMap(tmpDir, f.scanFrequency, f.forceChan, f.exitChan)
	f.forceChan <- struct{}{}
	time.Sleep(2 * time.Second)

	//add metrics to cache
	for i, metricName := range addMetrics {
		// f.testCache.Add(points.OnePoint(metricName, float64(i), int64(now-60)))
		f.testCache.Add(points.OnePoint(metricName, float64(i), 10))
	}

	//check expandblobs for new metrics
	f.checkExpandGlobs(t, addMetrics[2], false)
	f.checkExpandGlobs(t, addMetrics[0], false)

	//pop metric from cache
	m1 := f.testCache.WriteoutQueue().Get(nil)
	p1, _ := f.testCache.PopNotConfirmed(m1)
	f.testCache.Confirm(p1)

	if !p1.Eq(points.OnePoint(addMetrics[4], 4, 10)) {
		fmt.Printf("error - recived wrong point - %v\n", p1)
		t.FailNow()
	}

	for _, filePath := range removeFiles {
		if err = removeFileFromDir(filePath, tmpDir); err != nil {
			fmt.Errorf("error removing file from temp directory - %s\n error is: %v\n", filePath, err)
			t.Fatal(err)
		}
	}

	time.Sleep(5 * time.Second)
	f.checkExpandGlobs(t, "path.to.file.name1", true)
	f.checkExpandGlobs(t, addMetrics[3], true)
	f.checkExpandGlobs(t, addMetrics[0], true)
	f.checkExpandGlobs(t, addMetrics[4], false)

	//pop metric from cache
	m2 := f.testCache.WriteoutQueue().Get(nil)
	p2, _ := f.testCache.PopNotConfirmed(m2)
	f.testCache.Confirm(p2)

	// queue within cache is sorted by length of metric name for some reason
	if !p2.Eq(points.OnePoint(addMetrics[0], 0, 10)) {
		fmt.Printf("error - recived wrong point - %v\n", p2)
		t.FailNow()
	}
	f.checkExpandGlobs(t, addMetrics[0], true)

	//wait for next filewalk and check the metric again
	fmt.Println("wait for next filewalk and check the metric again")
	time.Sleep(4 * time.Second)
	f.checkExpandGlobs(t, addMetrics[0], false)

	close(f.exitChan)
}
