package metrics
//
// import (
// 	"fmt"
// 	"io/ioutil"
// 	"os"
// 	"path/filepath"
// 	"testing"
// 	"time"
// 	// "strings"
// 	// "github.com/lomik/go-carbon/helper/qa"
// 	// "github.com/lomik/go-carbon/points"
// )
//
// var addFiles = [...]string{
// 	"path/to/file/name1.wsp",
// 	"path/to/file/name2.wsp",
// 	"path/to/file1/name1.wsp",
// 	"file/name1.wsp",
// 	"justname.wsp",
// }
//
// var removeFiles = [...]string{
// 	"path/to/file/name2.wsp",
// 	"path/to/file1/name1.wsp",
// 	"justname.wsp",
// }
//
// type testInfo struct {
// 	fileScanObj     *fileScan
// 	indexUpdateChan chan MetricUpdate
// 	forceChan       chan struct{}
// 	exitChan        chan struct{}
// }
//
// func addFilePathToDir(filePath string, tmpDir string) error {
// 	err := os.MkdirAll(filepath.Join(tmpDir, filePath), 0755)
// 	if err != nil {
// 		os.RemoveAll(tmpDir)
// 	}
// 	return err
// }
//
// func removeFileFromDir(filePath string, tmpDir string) error {
// 	return os.Remove(filepath.Join(tmpDir, filePath))
// }
//
// func getTestDir() (string, error) {
// 	tmpDir, err := ioutil.TempDir("", "")
// 	if err != nil {
// 		return "", fmt.Errorf("error creating temp directory: %v\n", err)
// 	}
// 	return tmpDir, nil
// }
//
// func oldfileWalkHelper(t *testing.T, dir string) {
// 	fmt.Fprintln(os.Stderr, ";;;;;;;;;;;; entered filwalk run in oldfileWalkHelper -  ", dir)
// 	c := 1
// 	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
// 		if err != nil {
// 			fmt.Printf("prevent panic by handling failure accessing a path %q: %v\n", path, err)
// 			return err
// 		}
// 		if c == 1 {
// 			fmt.Fprintln(os.Stderr, ";;;;;;;;;;;; started here -  ", dir)
// 			fmt.Fprintln(os.Stderr, "visited file info", info)
// 		}
// 		fmt.Printf("visited file or dir: %q\n", info.Name())
//
// 		c++
// 		return nil
// 	})
// 	if err != nil {
// 		fmt.Printf("error walking the path: %v\n", err)
// 		t.Fatal(err)
// 	}
// }
//
// func getTestInfo(dir string) *testInfo {
// 	configFile := TestConfig(dir)
// 	app := New(configFile)
// 	scanTime := 3 * time.Second
// 	indexUptChan := make(chan MetricUpdate, 20)
// 	return &testInfo{
// 		indexUpdateChan: indexUptChan,
// 		forceChan:       make(chan struct{}),
// 		exitChan:        make(chan struct{}),
// 		fileScanObj:     app.fileScan(indexUptChan, scanTime, dir),
// 	}
// }
//
// func TestFileScan(t *testing.T) {
// 	tmpDir, err := getTestDir()
// 	if err != nil {
// 		fmt.Printf("unable to create test dir tree: %v\n", err)
// 		t.Fatal(err)
// 	}
// 	defer os.RemoveAll(tmpDir)
//
// 	for _, filePath := range addFiles {
// 		if err = addFilePathToDir(filePath, tmpDir); err != nil {
// 			fmt.Errorf("error creating temp directory for - %s\n error is: %v\n", filePath, err)
// 			t.Fatal(err)
// 		}
// 	}
//
// 	fmt.Println("temp dir is at - ", tmpDir)
//
// 	f := getTestInfo(tmpDir)
// 	go f.fileScanObj.runFileWalk(f.forceChan, f.exitChan)
// 	f.forceChan <- struct{}{}
// 	time.Sleep(5 * time.Second)
//
// 	for _, filePath := range removeFiles {
// 		if err = removeFileFromDir(filePath, tmpDir); err != nil {
// 			fmt.Errorf("error removing file from temp directory - %s\n error is: %v\n", filePath, err)
// 			t.Fatal(err)
// 		}
// 	}
// 	time.Sleep(2 * time.Second)
// 	f.exitChan <- struct{}{}
//
// 	chanLen := len(f.indexUpdateChan)
// 	fmt.Println("len of indexupdate channel - ", chanLen)
// 	for i := 0; i < chanLen; i++ {
// 		fmt.Fprintln(os.Stderr, "the value is ", <-f.indexUpdateChan)
// 	}
// 	// oldfileWalkHelper(t,tmpDir)
// }
