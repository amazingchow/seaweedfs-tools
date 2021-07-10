package main

import (
	param_parser "flag"
	"io"
	"os"
	"path"
	"strconv"

	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/needle_map"
	"github.com/sirupsen/logrus"
)

var (
	_SrcDir     = param_parser.String("src", "/mnt/locals/seeweedfsvolume/volume0/volume", "directory to store volume data files used by seaweedfs.")
	_DstDir     = param_parser.String("dst", "/mnt/locals/seeweedfsvolume/volume0/volume-output", "directory to store regenerated volume data files.")
	_Collection = param_parser.String("collection", "", "the volume collection name.")
	_VolumeId   = param_parser.Int("vid", -1, "the volume id, the volume .dat and .idx files should already exist inside the src dir.")
	_Limit      = param_parser.Int("limit", 0, "only show first n entries if specified.")
)

func main() {
	param_parser.Parse()

	if *_Collection == "" || *_VolumeId == -1 {
		logrus.Warning("nothing to do!!!")
		return
	}

	var err error

	filename := *_Collection + "_" + strconv.Itoa(*_VolumeId)
	idxFile := filename + ".idx"
	datFile := filename + ".dat"

	srcNM := needle_map.NewMemDb()
	defer srcNM.Close()
	// read from source idx file
	if err = srcNM.LoadFromIdx(path.Join(*_SrcDir, idxFile)); err != nil {
		logrus.Fatalf("failed to load needle map from %s, err: %v", path.Join(*_SrcDir, idxFile), err)
	}
	dstNM := needle_map.NewMemDb()
	defer dstNM.Close()

	logrus.Infof("ready to parse %s", idxFile)

	vid := needle.VolumeId(*_VolumeId)
	volumeFileScanner := &VolumeFileScanner4Transformer{
		Vid:          vid,
		SrcNeedleMap: srcNM,
		DstNeedleMap: dstNM,
		DstDataFile:  path.Join(*_DstDir, datFile),
	}
	err = storage.ScanVolumeFile(*_SrcDir, *_Collection, vid, storage.NeedleMapInMemory, volumeFileScanner)
	if err != nil && err != io.EOF {
		logrus.Fatalf("failed to scan volume file, err: %v \n", err)
	}
	volumeFileScanner.Close()

	// write to destination idx file
	if err = dstNM.SaveToIdx(path.Join(*_DstDir, idxFile)); err != nil {
		logrus.Fatalf("failed to save needle map to %s, err: %v", path.Join(*_DstDir, idxFile), err)
		_ = os.Remove(path.Join(*_DstDir, idxFile))
	}
}
