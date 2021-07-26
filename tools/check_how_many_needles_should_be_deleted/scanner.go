package main

import (
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/needle_map"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/sirupsen/logrus"
)

// 实现seaweedfs的VolumeFileScanner接口
type VolumeFileScanner4Transformer struct {
	version         needle.Version
	counter         int64
	shouldBeDeleted int64

	SrcNeedleMap *needle_map.MemDb
	OlderThan    int64

	ExitErr error
}

func (scanner *VolumeFileScanner4Transformer) VisitSuperBlock(superBlock super_block.SuperBlock) error {
	scanner.version = superBlock.Version
	return nil
}

func (scanner *VolumeFileScanner4Transformer) ReadNeedleBody() bool {
	return true
}

func (scanner *VolumeFileScanner4Transformer) VisitNeedle(srcNeedle *needle.Needle, offset int64, _, _ []byte) error {
	nv, ok := scanner.SrcNeedleMap.Get(srcNeedle.Id)
	if ok && nv.Size > 0 && nv.Size != types.TombstoneFileSize && nv.Offset.ToAcutalOffset() == offset {
		scanner.counter++
		if scanner.OlderThan >= 0 && srcNeedle.HasLastModifiedDate() && srcNeedle.LastModified < uint64(scanner.OlderThan) {
			scanner.shouldBeDeleted++
			return nil
		}
	}
	if !ok {
		logrus.Warningf("this needle <%d> seems to be deleted already", srcNeedle.Id)
	}
	return nil
}

func (scanner *VolumeFileScanner4Transformer) Counter() int64 {
	return scanner.counter
}

func (scanner *VolumeFileScanner4Transformer) ShouldBeDeleted() int64 {
	return scanner.shouldBeDeleted
}
