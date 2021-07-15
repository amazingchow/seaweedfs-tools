package main

import (
	param_parser "flag"
	"io"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/needle_map"
	"github.com/sirupsen/logrus"
)

var (
	_SrcDir = param_parser.String("src",
		"/mnt/locals/seeweedfsvolume/volume0/volume",
		"directory to store volume data files, the .idx and .dat files should already exist inside the dir.")
	_DstDir = param_parser.String("dst",
		"/mnt/locals/seeweedfsvolume/volume0/volume-output",
		"directory to store encrypted volume data files.")
	_Collection = param_parser.String("collection",
		"",
		"the volume collection name.")
	_VolumeId = param_parser.Int("vid",
		-1,
		"the volume id.")
	_Newer = param_parser.String("newer",
		"",
		"export only files newer than this time, default is all files. Must be specified in RFC3339 without timezone, e.g. 2006-01-02T15:04:05.")
	_TimeZone = param_parser.String("tz",
		"",
		"timezone, e.g. Asia/Shanghai.")
	_Verbose = param_parser.Bool("verbose",
		false,
		"verbose")
)

func main() {
	param_parser.Parse()

	if *_Verbose {
		logrus.SetLevel(logrus.DebugLevel)
	}

	if *_Collection == "" || *_VolumeId == -1 || *_Newer == "" {
		logrus.Warning("no collection or volume id or newer time provided")
		return
	}

	localLocation, err := time.LoadLocation("Local")
	if err != nil {
		logrus.Fatalf("failed to load time location, err: %v", err)
	}
	if *_TimeZone != "" {
		localLocation, err = time.LoadLocation(*_TimeZone)
		if err != nil {
			logrus.Fatalf("failed to load time location, err: %v", err)
		}
	}
	newerThan, err := time.ParseInLocation("2006-01-02T15:04:05", *_Newer, localLocation)
	if err != nil {
		logrus.Fatalf("failed to parse time, err: %v", err)
	}
	newerThanUnix := newerThan.Unix()

	// 只需生成.idx文件和.dat文件, 可以复用原先的.vif文件
	filename := *_Collection + "_" + strconv.Itoa(*_VolumeId)
	idxFile := filename + ".idx"
	datFile := filename + ".dat"

	// needle map缓存needle索引信息, key = []byte(NeedleId), value = []byte(Offset + Size)
	srcNM := needle_map.NewMemDb()
	defer srcNM.Close()
	if err = srcNM.LoadFromIdx(path.Join(*_SrcDir, idxFile)); err != nil {
		logrus.Fatalf("failed to load needle map from %s, err: %v", path.Join(*_SrcDir, idxFile), err)
	}
	dstNM := needle_map.NewMemDb()
	defer dstNM.Close()

	logrus.Infof("ready to parse %s", path.Join(*_SrcDir, datFile))

	vid := needle.VolumeId(*_VolumeId)
	volumeFileScanner := &VolumeFileScanner4Cleaner{
		SrcNeedleMap: srcNM,
		DstNeedleMap: dstNM,
		DstDataFile:  path.Join(*_DstDir, datFile),
		NewerThan:    newerThanUnix,
	}
	err = storage.ScanVolumeFile(*_SrcDir, *_Collection, vid, storage.NeedleMapInMemory, volumeFileScanner)
	if err != nil && err != io.EOF {
		if volumeFileScanner.ExitErr != ErrCreateDataFile {
			volumeFileScanner.Close()
			_ = os.Remove(path.Join(*_DstDir, datFile))
		}
		logrus.Fatalf("failed to scan %s, err: %v", path.Join(*_SrcDir, datFile), err)
	}
	volumeFileScanner.Close()

	// 生成新的.idx文件
	if err = dstNM.SaveToIdx(path.Join(*_DstDir, idxFile)); err != nil {
		logrus.Fatalf("failed to save needle map to %s, err: %v", path.Join(*_DstDir, idxFile), err)
		_ = os.Remove(path.Join(*_DstDir, idxFile))
	}

	logrus.Infof("finish to parse %s", path.Join(*_SrcDir, datFile))
	logrus.Infof("totally processed %d needles", volumeFileScanner.Counter())
}
