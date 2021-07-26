package main

import (
	param_parser "flag"
	"io"
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
	_Collection = param_parser.String("collection",
		"",
		"the volume collection name.")
	_VolumeId = param_parser.Int("vid",
		-1,
		"the volume id.")
	_Older = param_parser.String("older",
		"",
		"how many files will be deleted older than this time, must be specified in RFC3339 without timezone, e.g. 2006-01-02T15:04:05.")
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

	if *_Collection == "" || *_VolumeId == -1 {
		logrus.Warning("no collection or volume id provided")
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
	olderThan, err := time.ParseInLocation("2006-01-02T15:04:05", *_Older, localLocation)
	if err != nil {
		logrus.Fatalf("failed to parse time, err: %v", err)
	}
	olderThanUnix := olderThan.Unix()

	filename := *_Collection + "_" + strconv.Itoa(*_VolumeId)
	idxFile := filename + ".idx"
	datFile := filename + ".dat"

	srcNM := needle_map.NewMemDb()
	defer srcNM.Close()
	if err = srcNM.LoadFromIdx(path.Join(*_SrcDir, idxFile)); err != nil {
		logrus.Fatalf("failed to load needle map from %s, err: %v", path.Join(*_SrcDir, idxFile), err)
	}

	logrus.Infof("ready to parse %s", path.Join(*_SrcDir, datFile))

	vid := needle.VolumeId(*_VolumeId)
	volumeFileScanner := &VolumeFileScanner4Transformer{
		SrcNeedleMap: srcNM,
		OlderThan:    olderThanUnix,
	}
	err = storage.ScanVolumeFile(*_SrcDir, *_Collection, vid, storage.NeedleMapInMemory, volumeFileScanner)
	if err != nil && err != io.EOF {
		logrus.Fatalf("failed to scan %s, err: %v", path.Join(*_SrcDir, datFile), err)
	}

	logrus.Infof("finish to parse %s", path.Join(*_SrcDir, datFile))
	logrus.Infof("totally processed %d needles", volumeFileScanner.Counter())
	logrus.Infof("there are %d needles should be deleted", volumeFileScanner.ShouldBeDeleted())
}
