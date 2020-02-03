// +build skipchan

package carbonserver

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
  "context"
	"regexp"
	"github.com/lomik/go-carbon/helper/metrics"
	"github.com/lomik/go-carbon/points"
  "github.com/lomik/go-carbon/cache"
	"github.com/lomik/go-carbon/persister"
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

type testInfo struct {
	fileScanObj     *metrics.FileScan
	indexUpdateChan chan metrics.MetricUpdate
	forceChan       chan struct{}
	exitChan        chan bool
  testCache       *cache.Cache
  csListener      *CarbonserverListener
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
	scanTime := 1 * time.Second
	indexUptChan := make(chan metrics.MetricUpdate, 100)
  c := cache.New(indexUptChan)
  carbonserver := NewCarbonserverListener(c.Get,indexUptChan,getMetricRetentionAggregation)
  carbonserver.whisperData = dir
	carbonserver.trigramIndex = true
	// carbonserver.trieIndex = true
	carbonserver.scanFrequency = 3 * time.Second
  carbonserver.logger = zap.NewNop()
  carbonserver.metrics = &metricStruct{}
  carbonserver.exitChan = make(chan struct{})

	return &testInfo{
		indexUpdateChan: indexUptChan,
		forceChan:       make(chan struct{}),
		exitChan:        make(chan bool),
		fileScanObj:     metrics.NewFileScan(indexUptChan, scanTime, dir),
    testCache:       c,
    csListener:      carbonserver,
	}
}

func (f *testInfo) checkexpandblobs(t *testing.T, query string){
  fmt.Println("the query is - ",query)
  expandedGlobs, err := f.csListener.getExpandedGlobs(context.TODO(), zap.NewNop(), time.Now(), []string{query})
	if err != nil {
		t.Errorf("Unexpected err: '%v', expected: 'nil'", err)
		return
	}

    if expandedGlobs == nil {
        t.Errorf("No globs returned")
        return
    }

    fmt.Println("************* the expanded globs - ",expandedGlobs[0])
		fmt.Printf("************* expanded globs struct - %#v\n",expandedGlobs)
}

func TestIndexUpdateOverChannel(t *testing.T) {
	tmpDir, err := getTestDir()
	if err != nil {
		fmt.Printf("unable to create test dir tree: %v\n", err)
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	for _, filePath := range addFiles {
		if err = addFilePathToDir(filePath, tmpDir); err != nil {
			fmt.Errorf("error creating temp directory for - %s\n error is: %v\n", filePath, err)
			t.Fatal(err)
		}
	}

  // get test info
	f := getTestInfo(tmpDir)

  //start filewalk
	go f.fileScanObj.RunFileWalk(f.forceChan, f.exitChan)
	f.forceChan <- struct{}{}

  //start indexupdater
  idxUpdater := f.csListener.indexUpdater()
  go idxUpdater.updateIndex()

  //add metrics to cache
  for i, metricName := range addMetrics{
    f.testCache.Add(points.OnePoint(metricName, float64(i), int64(now-60)))
  }

  // idxUpdater := f.csListener.indexUpdater()
  // go idxUpdater.updateIndex()

  //check expandblobs for new metrics
	// fmt.Println("querying expand blobs for file - ", addFiles[0])
	// f.checkexpandblobs(t,addFiles[0])
	// fmt.Println("querying expand blobs for cache only metric - ", addMetrics[2])
  // f.checkexpandblobs(t,addMetrics[2])
	// fmt.Println("querying expand blobs for cache only metric - ", addMetrics[0])
  // f.checkexpandblobs(t,addMetrics[0])
	fmt.Println("====================> sleeping for 3 seonds")
	time.Sleep(3 * time.Second)
	fmt.Println("====================> slept for 3 seonds")


	for _, filePath := range removeFiles {
		fmt.Println("====================> deleting this file", filePath)
		if err = removeFileFromDir(filePath, tmpDir); err != nil {
			fmt.Errorf("error removing file from temp directory - %s\n error is: %v\n", filePath, err)
			t.Fatal(err)
		}
	}
	time.Sleep(2 * time.Second)

	fmt.Println("querying expand blobs for file - path.to.file.name1")
	f.checkexpandblobs(t,"path.to.file.nam*1")
	fmt.Println("querying expand blobs for cache only metric - ", addMetrics[3])
  f.checkexpandblobs(t,"new.data.poi*t1")
	// fmt.Println("querying expand blobs for cache only metric - ", addMetrics[2])
	// f.checkexpandblobs(t,addMetrics[2])
	// fmt.Println("querying expand blobs for cache only metric - ", addMetrics[0])
	// f.checkexpandblobs(t,addMetrics[0])
	time.Sleep(2 * time.Second)
	f.exitChan <- true

  //
	// chanLen := len(f.indexUpdateChan)
	// fmt.Println("len of indexupdate channel - ", chanLen)
	// for i := 0; i < chanLen; i++ {
	// 	fmt.Fprintln(os.Stderr, "the value is ", <-f.indexUpdateChan)
	// }
}
