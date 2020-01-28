/*
 * Copyright 2013-2016 Fabian Groffen, Damian Gryski, Vladimir Smirnov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package carbonserver

import (
	"fmt"
	_ "net/http/pprof"
	"os"
	// "path/filepath"
	// "strings"
	"sync/atomic"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/dgryski/go-trigram"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/helper/metrics"
)

type indexInfo struct {
	helper.Stoppable
	scan          <-chan time.Time
	csListener    *CarbonserverListener
	metricsMap    map[string]struct{}
	idxUpdateChan chan metrics.MetricUpdate
	fileWalkInfo  *metrics.FileWalkResp
}

type fileIndex struct {
	typ int

	idx   trigram.Index
	files []string

	trieIdx *trieIndex

	details     map[string]*protov3.MetricDetails
	accessTimes map[string]int64
	freeSpace   uint64
	totalSpace  uint64
}

// func NewIndexUpdater(fileScanFrequency time.Duration, fidx *fileIndex, whisperData string) *indexInfo {
func (listener *CarbonserverListener) indexUpdater() *indexInfo {
	return &indexInfo{
		// Config variables
		scan:          time.Tick(listener.scanFrequency),
		csListener:    listener,
		idxUpdateChan: listener.indexUpdateChan,
		metricsMap:    make(map[string]struct{}),
		fileWalkInfo: &metrics.FileWalkResp{
			Details: make(map[string]*protov3.MetricDetails),
		},
	}
}

func (indexUpdater *indexInfo) UpdateFileIndex(fidx *fileIndex) {
	indexUpdater.csListener.fileIdx.Store(fidx)
}

/*
// func (indexUpdater *indexInfo) updateIndex() {
// 	for {
// 	fmt.Fprintln(os.Stderr, "*************** Reading from carbonserver")
// 		select {
// 		case <-indexUpdater.csListener.exitChan:
// 			fmt.Fprintln(os.Stderr, "*************** exit case")
// 			return
// 		case <-indexUpdater.cpyOvrChan:
// 			// create/update the trie index
// 			fmt.Fprintln(os.Stderr, "*************** populate index case")
// 			// doubt: do I have to handle the case to close the call to this go routine?
// 			go indexUpdater.populateFileIndex(indexUpdater.fileWalkInfo)
// 		// case val := <-indexUpdater.idxUpdateChan:
// 		case <-indexUpdater.idxUpdateChan:
// 			// doubt: add sync.RWMutex ??
// 			// doubt: do we have to suffix ".wsp" to every metric, if so,
// 			// 				do we have to convert every metric to this - https://play.golang.org/p/x6OtSk-0b7R
// 			// indexUpdater.metricsMap[val] = true
// 			fmt.Fprintln(os.Stderr, "*************** Read from carbonserver")
// 			fmt.Fprintln(os.Stderr,"*************** current index :", indexUpdater.metricsMap)
// 		case <-indexUpdater.csListener.forceScanChan:
// 			go indexUpdater.runFileWalk(indexUpdater.csListener.whisperData)
// 		case <-indexUpdater.scan:
// 			// doubt: do I have to handle the case to close the call to this go routine?
// 			go indexUpdater.runFileWalk(indexUpdater.csListener.whisperData)
// 		}
// 		fmt.Fprintln(os.Stderr, "*************** Done reading from carbonserver")
// 	}
// }
*/

func (indexUpdater *indexInfo) updateIndex() {
	for {
		select {
		case <-indexUpdater.csListener.exitChan:
			fmt.Fprintln(os.Stderr, "*************** exit case")
			return
		case update := <-indexUpdater.idxUpdateChan:
			// fmt.Fprintln(os.Stderr, "*************** current index :", indexUpdater.metricsMap)
			switch update.Operation {
			case metrics.ADD:
				// append to index
				fmt.Fprintln(os.Stderr, "*************** IndexUpdater received ADD operation ")
				indexUpdater.metricsMap[update.Name] = struct{}{}
			case metrics.DEL:
				//delete from index
				// https://stackoverflow.com/a/1736032
				// fmt.Fprintln(os.Stderr, "*************** IndexUpdater received DEL operation ")
				delete(indexUpdater.metricsMap, update.Name)
			case metrics.FLUSH:
				// populate trie
				fmt.Fprintln(os.Stderr, "*************** IndexUpdater received FLUSH operation ")
				// fmt.Fprintln(os.Stderr, "*************** current index :", indexUpdater.metricsMap)
				fmt.Fprintln(os.Stderr, "*************** len of current index :", len(indexUpdater.metricsMap))
				indexUpdater.fileWalkInfo = update.FileWalkInfo
				//update files with metrics from cache ADDs
				indexUpdater.fileWalkInfo.Files = indexUpdater.metricsMap
				indexUpdater.populateFileIndex()
			}
		}
	}
}

/*
func (indexUpdater *indexInfo) runFileWalk(dir string) {
	logger := indexUpdater.csListener.logger.With(zap.String("handler", "runFileWalk"))
	defer func() {
		if r := recover(); r != nil {
			logger.Error("panic encountered",
				zap.Stack("stack"),
				zap.Any("error", r),
			)
		}
	}()

	_ = indexUpdater.fileWalkInfo //info from previous walk
	currWalk := &fileWalkResp{
		details:    make(map[string]*protov3.MetricDetails),
		whisperDir: dir,
	}

	err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			logger.Info("error processing", zap.String("path", p), zap.Error(err))
			return nil
		}

		hasSuffix := strings.HasSuffix(info.Name(), ".wsp")
		if info.IsDir() || hasSuffix {
			trimmedName := strings.TrimPrefix(p, indexUpdater.csListener.whisperData)
			currWalk.files = append(currWalk.files, trimmedName)
			if hasSuffix {
				currWalk.metricsKnown++
				if indexUpdater.csListener.internalStatsDir != "" {
					i := stat.GetStat(info)
					trimmedName = strings.Replace(trimmedName[1:len(trimmedName)-4], "/", ".", -1)
					currWalk.details[trimmedName] = &protov3.MetricDetails{
						Size_:    i.Size,
						ModTime:  i.MTime,
						ATime:    i.ATime,
						RealSize: i.RealSize,
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		logger.Error("error getting file list",
			zap.Error(err),
		)
	}
	// return currWalk
	indexUpdater.fileWalkInfo = currWalk
	indexUpdater.cpyOvrChan <- struct{}{}
}
*/

func (indexUpdater *indexInfo) populateFileIndex() {
	fmt.Fprintln(os.Stderr, "========= reaches populateFileIndex")
	logger := indexUpdater.csListener.logger.With(zap.String("handler", "fileListUpdated"))
	defer func() {
		if r := recover(); r != nil {
			logger.Error("panic encountered",
				zap.Stack("stack"),
				zap.Any("error", r),
			)
		}
	}()
	t0 := time.Now()
	var stat syscall.Statfs_t
	err := syscall.Statfs(indexUpdater.fileWalkInfo.WhisperDir, &stat)
	if err != nil {
		logger.Info("error getting FS Stats",
			zap.String("dir", indexUpdater.fileWalkInfo.WhisperDir),
			zap.Error(err),
		)
		return
	}

	var freeSpace uint64
	if stat.Bavail >= 0 {
		freeSpace = uint64(stat.Bavail) * uint64(stat.Bsize)
	}
	totalSpace := stat.Blocks * uint64(stat.Bsize)

	fileScanRuntime := time.Since(t0)
	atomic.StoreUint64(&indexUpdater.csListener.metrics.MetricsKnown, indexUpdater.fileWalkInfo.MetricsKnown)
	atomic.AddUint64(&indexUpdater.csListener.metrics.FileScanTimeNS, uint64(fileScanRuntime.Nanoseconds()))

	nfidx := &fileIndex{
		details:     indexUpdater.fileWalkInfo.Details,
		freeSpace:   freeSpace,
		totalSpace:  totalSpace,
		accessTimes: make(map[string]int64),
	}
	var infos []zap.Field
	infos = append(infos,
		zap.Duration("file_scan_runtime", fileScanRuntime),
		zap.Int("Files", len(indexUpdater.fileWalkInfo.Files)),
	)
	indexUpdater.updateTrie(nfidx, infos, logger)
}

func (indexUpdater *indexInfo) updateTrie(nfidx *fileIndex, infos []zap.Field, logger *zap.Logger) {
	var pruned int
	fmt.Fprintln(os.Stderr, "========= reaches updateTrie")
	var indexType = "trigram"
	t0 := time.Now()
	// fmt.Fprintln(os.Stderr, "infos for now:", infos)
	if indexUpdater.csListener.trieIndex {
		indexType = "trie"
		nfidx.trieIdx = newTrie(".wsp")
		var errs []error
		//range over files
		for file := range indexUpdater.fileWalkInfo.Files {
			if err := nfidx.trieIdx.insert(file); err != nil {
				errs = append(errs, err)
			}
		}
		fmt.Fprintln(os.Stderr, "========= trieIndex", nfidx.trieIdx)
		infos = append(
			infos,
			zap.Int("trie_depth", nfidx.trieIdx.depth),
			zap.String("longest_metric", nfidx.trieIdx.longestMetric),
		)
		if len(errs) > 0 {
			infos = append(infos, zap.Errors("trie_index_errors", errs))
		}
		if indexUpdater.csListener.trigramIndex {
			start := time.Now()
			nfidx.trieIdx.setTrigrams()
			infos = append(infos, zap.Duration("set_trigram_time", time.Now().Sub(start)))
		}
	} else {
		currFiles := make([]string, len(indexUpdater.fileWalkInfo.Files))
		i := 0
		for file := range indexUpdater.fileWalkInfo.Files {
			currFiles[i] = file
			i++
		}
		// fmt.Println("curr files : ",currFiles)
		// fmt.Fprintln(os.Stderr, "========= trigram")
		nfidx.files = currFiles
		nfidx.idx = trigram.NewIndex(currFiles)
		// fmt.Println("trigram index : ",nfidx.idx)
		pruned = nfidx.idx.Prune(0.95)
	}
	indexSize := len(nfidx.idx)
	indexingRuntime := time.Since(t0)
	atomic.AddUint64(&indexUpdater.csListener.metrics.IndexBuildTimeNS, uint64(indexingRuntime.Nanoseconds()))

	tl := time.Now()

	fidx := indexUpdater.csListener.CurrentFileIndex()

	if fidx != nil && indexUpdater.csListener.internalStatsDir != "" {
		indexUpdater.csListener.fileIdxMutex.Lock()
		for m := range fidx.accessTimes {
			if d, ok := indexUpdater.fileWalkInfo.Details[m]; ok {
				d.RdTime = fidx.accessTimes[m]
			} else {
				delete(fidx.accessTimes, m)
				if indexUpdater.csListener.db != nil {
					indexUpdater.csListener.db.Delete([]byte(m), nil)
				}
			}
		}
		nfidx.accessTimes = fidx.accessTimes
		indexUpdater.csListener.fileIdxMutex.Unlock()
	}
	rdTimeUpdateRuntime := time.Since(tl)

	indexUpdater.UpdateFileIndex(nfidx)
	fmt.Fprintln(os.Stderr, "========= updated the fileIndex in updateTrie")

	infos = append(infos,
		zap.Duration("indexing_runtime", indexingRuntime),
		zap.Duration("rdtime_update_runtime", rdTimeUpdateRuntime),
		zap.Int("index_size", indexSize),
		zap.Int("pruned_trigrams", pruned),
		zap.String("index_type", indexType),
		zap.Duration("total_runtime", time.Since(t0)),
	)
	fmt.Fprintln(os.Stderr, "========= fileIndex updatetime - ", indexingRuntime)

	logger.Info("file list updated", infos...)
}

/*
// func (indexUpdater *indexInfo) fileListUpdater() {
// 	for {
// 		select {
// 		case <-indexUpdater.csListener.exitChan:
// 			return
// 		case <-indexUpdater.scan:
// 		case <-indexUpdater.csListener.forceScanChan:
// 		}
// 		fmt.Fprintln(os.Stderr,"========= reaches fileListUpdater")
// 		indexUpdater.updateFileList(indexUpdater.csListener.whisperData)
// 	}
// }
*/
