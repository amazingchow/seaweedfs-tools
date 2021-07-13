package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/needle_map"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/sirupsen/logrus"

	myutils "github.com/amazingchow/seaweedfs-tools/pkg/utils"
)

var (
	ErrCreateDataFile           = errors.New("can not create new data file")
	ErrCreateNeedle             = errors.New("can not create new needle")
	ErrPrepareNeedleWriteBuffer = errors.New("can not prepare needle's write buffer")
	ErrGetDataFileWriteOffset   = errors.New("can not get write-offset")
	ErrWriteNeedleBytes         = errors.New("can not write needle bytes")
	ErrSetNeedleMap             = errors.New("can not set k/v for needle map")
)

type VolumeFileScanner4Transformer struct {
	Version needle.Version
	Counter int

	Vid            needle.VolumeId
	SrcNeedleMap   *needle_map.MemDb
	DstNeedleMap   *needle_map.MemDb
	DstDataFile    string
	DstDataBackend *backend.DiskFile

	CipherKey []byte

	ExitErr error
}

func (scanner *VolumeFileScanner4Transformer) VisitSuperBlock(superBlock super_block.SuperBlock) error {
	scanner.Version = superBlock.Version

	logrus.Debugf("create new data file <%s>", scanner.DstDataFile)
	fd, err := os.OpenFile(scanner.DstDataFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		logrus.Errorf("failed to create new data file, err: %v", err)
		scanner.ExitErr = ErrCreateDataFile
		return ErrCreateDataFile
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

func (scanner *VolumeFileScanner4Transformer) VisitNeedle(srcNeedle *needle.Needle, offset int64, needleHeader, needleBody []byte) error {
	nv, ok := scanner.SrcNeedleMap.Get(srcNeedle.Id)
	if ok && nv.Size > 0 && nv.Size != types.TombstoneFileSize && nv.Offset.ToAcutalOffset() == offset {
		logrus.Debugf("process needle, key %d offset %d size %d disk_size %d", srcNeedle.Id, offset, srcNeedle.Size, srcNeedle.DiskSize(scanner.Version))
		scanner.Counter++
		if *_Limit > 0 && scanner.Counter > *_Limit {
			return io.EOF
		}
		// 1. write the needle to .dat file
		// 1.1 create a new needle from the old one
		dstNeedle, err := scanner.CreateDstNeedle(srcNeedle)
		if err != nil {
			logrus.Errorf("failed to create new needle, err: %v", err)
			return ErrCreateNeedle
		}
		dstNeedle.AppendAtNs = uint64(time.Now().UnixNano())
		// 1.2 fill in the bytes array with the new needle
		bytesToWrite, _, _, err := dstNeedle.PrepareWriteBuffer(scanner.Version)
		if err != nil {
			logrus.Errorf("failed to prepare write buffer, err: %v", err)
			return ErrPrepareNeedleWriteBuffer
		}
		// 1.3 fetch the .dat file write-offset
		var offset int64
		if end, _, err := scanner.DstDataBackend.GetStat(); err == nil {
			offset = end
		} else {
			logrus.Errorf("failed to get write-offset, err: %v", err)
			return ErrGetDataFileWriteOffset
		}
		// 1.4 write the bytes array into backend
		if _, err = scanner.DstDataBackend.WriteAt(bytesToWrite, offset); err != nil {
			logrus.Errorf("failed to write needle bytes, err: %v", err)
			return ErrWriteNeedleBytes
		}
		// 2. write the needle index info to .idx file
		if err = scanner.DstNeedleMap.Set(dstNeedle.Id, types.ToOffset(int64(offset)), dstNeedle.Size); err != nil {
			logrus.Errorf("failed to set k/v for needle map, err: %v", err)
			return ErrSetNeedleMap
		}
	}
	if !ok {
		logrus.Debugf("this needle <%d> seems to be deleted", srcNeedle.Id)
	}
	return nil
}

func (scanner *VolumeFileScanner4Transformer) CreateDstNeedle(srcNeedle *needle.Needle) (dstNeedle *needle.Needle, err error) {
	dstNeedle = new(needle.Needle)
	dstNeedle.Cookie = srcNeedle.Cookie
	dstNeedle.Id = srcNeedle.Id
	dstNeedle.Data, err = myutils.Encrypt(srcNeedle.Data, scanner.CipherKey)

	now := time.Now()
	storedDays := uint32(((now.UnixNano() - int64(srcNeedle.AppendAtNs)) / 1e9) / 86400)
	days := srcNeedle.Ttl.ToUint32()>>8 - storedDays
	dstNeedle.Ttl, _ = needle.ReadTTL(fmt.Sprintf("%dd", days))

	if len(srcNeedle.Name) > 0 && len(srcNeedle.Name) < 256 {
		dstNeedle.Name = make([]byte, len(srcNeedle.Name))
		copy(dstNeedle.Name, srcNeedle.Name)
		dstNeedle.NameSize = uint8(len(dstNeedle.Name))
		dstNeedle.SetHasName()
	}

	if len(srcNeedle.Mime) > 0 && len(srcNeedle.Mime) < 256 {
		dstNeedle.Mime = make([]byte, len(srcNeedle.Mime))
		copy(dstNeedle.Mime, srcNeedle.Mime)
		dstNeedle.MimeSize = uint8(len(dstNeedle.Mime))
		dstNeedle.SetHasMime()
	}

	if len(srcNeedle.Pairs) > 0 && len(srcNeedle.Pairs) < 65536 {
		dstNeedle.Pairs = make([]byte, len(srcNeedle.Pairs))
		copy(dstNeedle.Pairs, srcNeedle.Pairs)
		dstNeedle.PairsSize = uint16(len(dstNeedle.Pairs))
		dstNeedle.SetHasPairs()
	}

	if srcNeedle.IsGzipped() {
		dstNeedle.SetGzipped()
	}

	if dstNeedle.LastModified == 0 {
		dstNeedle.LastModified = uint64(time.Now().Unix())
	}
	dstNeedle.SetHasLastModifiedDate()

	if dstNeedle.Ttl != needle.EMPTY_TTL {
		dstNeedle.SetHasTtl()
	}

	if srcNeedle.IsChunkedManifest() {
		dstNeedle.SetIsChunkManifest()
	}

	dstNeedle.Checksum = needle.NewCRC(dstNeedle.Data)

	return
}

func (scanner *VolumeFileScanner4Transformer) Close() {
	logrus.Infof("totally processed %d needles", scanner.Counter)
	_ = scanner.DstDataBackend.Close()
}
