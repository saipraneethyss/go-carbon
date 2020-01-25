package metrics

import (
  "fmt"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"

	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/zapwriter"
)

type OpType int

const (
	FLUSH OpType = iota
	ADD
	DEL
)

type FileScan struct {
	helper.Stoppable
	Scan          <-chan time.Time
	IdxUpdateChan chan MetricUpdate
	FileWalkInfo  *FileWalkResp
	Logger        *zap.Logger
	WhisperDir    string
}

type MetricUpdate struct {
	Name         string
	Operation    OpType
	FileWalkInfo *FileWalkResp
}

type FileWalkResp struct {
	Details      map[string]*protov3.MetricDetails
	MetricsKnown uint64
	Files        map[string]struct{}
	WhisperDir   string
}

func NewFileScan(indexUptChan chan MetricUpdate, scanFrequency time.Duration, dir string) *FileScan {
	return &FileScan{
		// Config variables
		IdxUpdateChan: indexUptChan,
		Scan:          time.Tick(scanFrequency),
		Logger:        zapwriter.Logger("fileScan"),
		WhisperDir:    dir,
	}
}

func (fScan *FileScan) RunFileWalk(forceChan <-chan struct{}, exitChan <-chan bool) {
	for {
		select {
		case <-exitChan:
			fmt.Fprintln(os.Stderr, "!!!!!!!! EXIT CASE !!!!!!!!!")
			return
		case <-fScan.Scan:
			fmt.Fprintln(os.Stderr, ";;;;;;;;;;;; entered scan frequency")
		case <-forceChan:
			fmt.Fprintln(os.Stderr, ";;;;;;;;;;;; entered forcescan")
		}
		fmt.Fprintln(os.Stderr, ";;;;;;;;;;;; about to run filewalk")
		fScan.fileWalkHelper()
		fmt.Fprintln(os.Stderr, ";;;;;;;;;;;; ran file walk")
	}
}

func (fScan *FileScan) currentFileWalkInfo() *FileWalkResp {
	return fScan.FileWalkInfo
}

func (fScan *FileScan) fileWalkHelper() {
	logger := fScan.Logger.With(zap.String("handler", "runFileWalk"))
	defer func() {
		if r := recover(); r != nil {
			logger.Error("panic encountered",
				zap.Stack("stack"),
				zap.Any("error", r),
			)
		}
	}()

	dir := fScan.WhisperDir

	currWalk := &FileWalkResp{
		Details:    make(map[string]*protov3.MetricDetails),
		Files:      make(map[string]struct{}),
		WhisperDir: dir,
	}

	var prevFiles map[string]struct{}
	// fmt.Fprintln(os.Stderr, "previous filewalk info - ", fScan.currentFileWalkInfo())
	if fScan.currentFileWalkInfo() != nil {
		prevFiles = fScan.FileWalkInfo.Files
    // fmt.Fprintln(os.Stderr, "previous files are - ", prevFiles)
    // fmt.Println( "previous files type is - %T", prevFiles)
	}

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			logger.Info("error processing", zap.String("path", path), zap.Error(err))
			return nil
		}
		hasSuffix := strings.HasSuffix(info.Name(), ".wsp")
		if info.IsDir() || hasSuffix {
			trimmedName := strings.TrimPrefix(path, dir)
			currWalk.Files[trimmedName] = struct{}{}
			exists := true
			if prevFiles != nil {
				_, exists = prevFiles[trimmedName]
			}
			if !exists || prevFiles == nil {
				// fmt.Fprintln(os.Stderr, "******=====****** ADD operation in FileWalk;sending this info to channel", trimmedName)
				fScan.IdxUpdateChan <- MetricUpdate{
					Name:      trimmedName,
					Operation: ADD,
				}
			}
			if hasSuffix {
				currWalk.MetricsKnown++
				// if indexUpdater.csListener.internalStatsDir != "" {
				// 			i := stat.GetStat(info)
				// 			trimmedName = strings.Replace(trimmedName[1:len(trimmedName)-4], "/", ".", -1)
				// 			currWalk.details[trimmedName] = &protov3.MetricDetails{
				// 				Size_:    i.Size,
				// 				ModTime:  i.MTime,
				// 				ATime:    i.ATime,
				// 				RealSize: i.RealSize,
				// 			}
				// 		}
			}
		}
		return nil
	})
	if err != nil {
		logger.Error("error getting file list",
			zap.Error(err),
		)
	}
	// fmt.Fprintln(os.Stderr, "======***====== completed OS fileWalk")
	// fmt.Fprintln(os.Stderr, "======***====== len of currfiles", len(currWalk.Files))
  // fmt.Fprintln(os.Stderr, "previous files are - ", prevFiles)

	for file, _ := range prevFiles {
		_, retain := currWalk.Files[file]
		if !retain {
			// fmt.Fprintln(os.Stderr, "******=====****** DEL operation;sending this info to channel", file)
			fScan.IdxUpdateChan <- MetricUpdate{
				Name:      file,
				Operation: DEL,
			}
		}
	}

	fScan.FileWalkInfo = currWalk
	// fmt.Fprintln(os.Stderr, "******=====****** FLUSH operation;")
	fScan.IdxUpdateChan <- MetricUpdate{
		Name:         "",
		Operation:    FLUSH,
		// FileWalkInfo: currWalk,
    FileWalkInfo: &FileWalkResp{
  		Details:    currWalk.Details,
  		WhisperDir: currWalk.WhisperDir,
  	},
	}
}
