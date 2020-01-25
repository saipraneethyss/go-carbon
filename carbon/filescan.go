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

package carbon

// import (
// 	"fmt"
// 	_ "net/http/pprof"
// 	"os"
// 	"path/filepath"
// 	"strings"
// 	// "sync/atomic"
// 	// "syscall"
// 	"time"
//
// 	"go.uber.org/zap"
//
// 	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
// 	"github.com/lomik/go-carbon/helper"
// 	"github.com/lomik/zapwriter"
// 	// "github.com/lomik/go-carbon/helper/stat"
// )
//
// type OpType int
//
// const (
// 	FLUSH OpType = iota
// 	ADD
// 	DEL
// )
//
// type fileScan struct {
// 	helper.Stoppable
// 	app           *App
// 	scan          <-chan time.Time
// 	idxUpdateChan chan MetricUpdate
// 	fileWalkInfo  *fileWalkResp
// 	logger        *zap.Logger
// 	whisperDir    string
// }
//
// type MetricUpdate struct {
// 	name         string
// 	operation    OpType
// 	fileWalkInfo *fileWalkResp
// }
//
// type fileWalkResp struct {
// 	details      map[string]*protov3.MetricDetails
// 	metricsKnown uint64
// 	files        map[string]struct{}
// 	whisperDir   string
// }
//
// func (carbonApp *App) fileScan(indexUptChan chan MetricUpdate, scanFrequency time.Duration, dir string) *fileScan {
// 	return &fileScan{
// 		// Config variables
// 		idxUpdateChan: indexUptChan,
// 		scan:          time.Tick(scanFrequency),
// 		app:           carbonApp,
// 		logger:        zapwriter.Logger("fileScan"),
// 		whisperDir:    dir,
// 	}
// }
//
// func (fScan *fileScan) runFileWalk(forceChan <-chan struct{}, exitChan <-chan struct{}) {
// 	for {
// 		select {
// 		case <-exitChan:
// 			fmt.Fprintln(os.Stderr, "!!!!!!!! EXIT CASE !!!!!!!!!")
// 			return
// 		case <-fScan.scan:
// 			fmt.Fprintln(os.Stderr, ";;;;;;;;;;;; entered scan frequency")
// 		case <-forceChan:
// 			fmt.Fprintln(os.Stderr, ";;;;;;;;;;;; entered forcescan")
// 		}
// 		fmt.Fprintln(os.Stderr, ";;;;;;;;;;;; about to run filewalk")
// 		fScan.fileWalkHelper()
// 		// fmt.Fprintln(os.Stderr,";;;;;;;;;;;; sleeping here again- ")
// 		// time.Sleep(3*time.Second)
// 		fmt.Fprintln(os.Stderr, ";;;;;;;;;;;; ran file walk")
// 	}
// }
//
// func (fScan *fileScan) CurrentFileWalkInfo() *fileWalkResp {
// 	return fScan.fileWalkInfo
// }
//
// func (fScan *fileScan) fileWalkHelper() {
// 	logger := fScan.logger.With(zap.String("handler", "runFileWalk"))
// 	defer func() {
// 		if r := recover(); r != nil {
// 			logger.Error("panic encountered",
// 				zap.Stack("stack"),
// 				zap.Any("error", r),
// 			)
// 		}
// 	}()
//
// 	dir := fScan.whisperDir
//
// 	currWalk := &fileWalkResp{
// 		details:    make(map[string]*protov3.MetricDetails),
// 		files:      make(map[string]struct{}),
// 		whisperDir: dir,
// 	}
//
// 	var prevFiles map[string]struct{}
// 	fmt.Fprintln(os.Stderr, "previous filewalk info - ", fScan.CurrentFileWalkInfo())
// 	if fScan.CurrentFileWalkInfo() != nil {
// 		prevFiles = fScan.fileWalkInfo.files
// 	}
//
// 	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
// 		if err != nil {
// 			logger.Info("error processing", zap.String("path", path), zap.Error(err))
// 			return nil
// 		}
// 		hasSuffix := strings.HasSuffix(info.Name(), ".wsp")
// 		if info.IsDir() || hasSuffix {
// 			trimmedName := strings.TrimPrefix(path, dir)
// 			currWalk.files[trimmedName] = struct{}{}
// 			exists := true
// 			if prevFiles != nil {
// 				_, exists = prevFiles[trimmedName]
// 			}
// 			if !exists || prevFiles == nil {
// 				fmt.Fprintln(os.Stderr, "******=====****** ADD operation;sending this info to channel", trimmedName)
// 				fScan.idxUpdateChan <- MetricUpdate{
// 					name:      trimmedName,
// 					operation: ADD,
// 				}
// 			}
// 			if hasSuffix {
// 				currWalk.metricsKnown++
// 				// if indexUpdater.csListener.internalStatsDir != "" {
// 				// 			i := stat.GetStat(info)
// 				// 			trimmedName = strings.Replace(trimmedName[1:len(trimmedName)-4], "/", ".", -1)
// 				// 			currWalk.details[trimmedName] = &protov3.MetricDetails{
// 				// 				Size_:    i.Size,
// 				// 				ModTime:  i.MTime,
// 				// 				ATime:    i.ATime,
// 				// 				RealSize: i.RealSize,
// 				// 			}
// 				// 		}
// 			}
// 		}
// 		return nil
// 	})
// 	if err != nil {
// 		logger.Error("error getting file list",
// 			zap.Error(err),
// 		)
// 	}
// 	fmt.Fprintln(os.Stderr, "======***====== completed OS fileWalk")
// 	fmt.Fprintln(os.Stderr, "======***====== len of currfiles", len(currWalk.files))
// 	for file, _ := range prevFiles {
// 		_, retain := currWalk.files[file]
// 		if !retain {
// 			fmt.Fprintln(os.Stderr, "******=====****** DEL operation;sending this info to channel", file)
// 			fScan.idxUpdateChan <- MetricUpdate{
// 				name:      file,
// 				operation: DEL,
// 			}
// 		}
// 	}
//
// 	fScan.fileWalkInfo = currWalk
// 	fmt.Fprintln(os.Stderr, "******=====****** FLUSH operation;")
// 	fScan.idxUpdateChan <- MetricUpdate{
// 		name:         "",
// 		operation:    FLUSH,
// 		fileWalkInfo: currWalk,
// 	}
// }
