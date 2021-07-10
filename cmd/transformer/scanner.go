package main

import (
	"io"
	"os"

	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/needle_map"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/sirupsen/logrus"
)

type VolumeFileScanner4Transformer struct {
	Version needle.Version
	Counter int

	Vid            needle.VolumeId
	SrcNeedleMap   *needle_map.MemDb
	DstNeedleMap   *needle_map.MemDb
	DstDataFile    string
	DstDataBackend *backend.DiskFile
}

func (scanner *VolumeFileScanner4Transformer) VisitSuperBlock(superBlock super_block.SuperBlock) error {
	scanner.Version = superBlock.Version

	fd, err := os.OpenFile(scanner.DstDataFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	scanner.DstDataBackend = backend.NewDiskFile(fd)
	_, err = scanner.DstDataBackend.WriteAt(superBlock.Bytes(), 0)
	if err != nil {
		scanner.DstDataBackend.Close()
		return err
	}
	return nil
}

func (scanner *VolumeFileScanner4Transformer) ReadNeedleBody() bool {
	return true
}

func (scanner *VolumeFileScanner4Transformer) VisitNeedle(n *needle.Needle, offset int64, needleHeader, needleBody []byte) error {
	nv, ok := scanner.SrcNeedleMap.Get(n.Id)
	if ok && nv.Size > 0 && nv.Size != types.TombstoneFileSize && nv.Offset.ToAcutalOffset() == offset {
		logrus.Debugf("process needle, key %d offset %d size %d disk_size %d", n.Id, offset, n.Size, n.DiskSize(scanner.Version))
		scanner.Counter++
		if *_Limit > 0 && scanner.Counter > *_Limit {
			return io.EOF
		}
		// 1. write the needle to .dat file
		// 2. write the needle index info to .idx file
	}
	if !ok {
		logrus.Debugf("this needle <%d> seems to be deleted", n.Id)
	}
	return nil
}

func (scanner *VolumeFileScanner4Transformer) Close() {
	_ = scanner.DstDataBackend.Close()
}
