package main

import (
	"errors"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/needle_map"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/sirupsen/logrus"
)

var (
	ErrCreateDataFile           = errors.New("can not create new data file")
	ErrCreateNeedle             = errors.New("can not create new needle")
	ErrPrepareNeedleWriteBuffer = errors.New("can not prepare needle's write buffer")
	ErrGetDataFileWriteOffset   = errors.New("can not get write-offset")
	ErrWriteNeedleBytes         = errors.New("can not write needle bytes")
	ErrSetNeedleMap             = errors.New("can not set k/v for needle map")
)

// 实现seaweedfs的VolumeFileScanner接口
type VolumeFileScanner4Transformer struct {
	version         needle.Version
	counter         int64
	shouldBeDeleted int64
	dstDataBackend  *backend.DiskFile
	httpClient      *http.Client

	SrcNeedleMap *needle_map.MemDb
	DstNeedleMap *needle_map.MemDb
	DstDataFile  string
	OlderThan    int64

	ExitErr error
}

func (scanner *VolumeFileScanner4Transformer) PrepareHttpClient() {
	scanner.httpClient = &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 10 * time.Second,
				DualStack: true,
			}).DialContext,
			MaxIdleConns:          50,
			MaxIdleConnsPerHost:   5,
			IdleConnTimeout:       1 * time.Hour,
			ResponseHeaderTimeout: 5 * time.Second,
			DisableCompression:    true,
		},
	}
}

func (scanner *VolumeFileScanner4Transformer) VisitSuperBlock(superBlock super_block.SuperBlock) error {
	scanner.version = superBlock.Version

	logrus.Debugf("create new data file %s", scanner.DstDataFile)
	file, err := os.OpenFile(scanner.DstDataFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		logrus.Errorf("failed to create new data file %s, err: %v", scanner.DstDataFile, err)
		scanner.ExitErr = ErrCreateDataFile
		return ErrCreateDataFile
	}
	scanner.dstDataBackend = backend.NewDiskFile(file)
	// TODO: 是否需要修改SuperBlock.Ttl?
	_, err = scanner.dstDataBackend.WriteAt(superBlock.Bytes(), 0)
	if err != nil {
		logrus.Errorf("failed to write needle bytes for super block, err: %v", err)
		scanner.ExitErr = ErrWriteNeedleBytes
		return ErrWriteNeedleBytes
	}
	return nil
}

func (scanner *VolumeFileScanner4Transformer) ReadNeedleBody() bool {
	return true
}

func (scanner *VolumeFileScanner4Transformer) VisitNeedle(srcNeedle *needle.Needle, offset int64, _, _ []byte) error {
	nv, ok := scanner.SrcNeedleMap.Get(srcNeedle.Id)
	if ok && nv.Size > 0 && nv.Size != types.TombstoneFileSize && nv.Offset.ToAcutalOffset() == offset {
		logrus.Debugf("process needle <id: %d | offset: %d | size: %d | disk_size: %d>",
			srcNeedle.Id, offset, srcNeedle.Size, srcNeedle.DiskSize(scanner.version))

		scanner.counter++

		// 1. write the needle to destination .dat file
		// 1.1 create a new needle from the old one
		dstNeedle, err := scanner.CreateDstNeedle(srcNeedle)
		if err != nil {
			logrus.Errorf("failed to create new needle, err: %v", err)
			scanner.ExitErr = ErrCreateNeedle
			return ErrCreateNeedle
		}
		if scanner.OlderThan >= 0 && srcNeedle.HasLastModifiedDate() && srcNeedle.LastModified < uint64(scanner.OlderThan) {
			scanner.shouldBeDeleted++
		}
		dstNeedle.AppendAtNs = uint64(time.Now().UnixNano())
		// 1.2 fill in the bytes array with the new needle
		bytesToWrite, _, _, err := dstNeedle.PrepareWriteBuffer(scanner.version)
		if err != nil {
			logrus.Errorf("failed to prepare write buffer, err: %v", err)
			scanner.ExitErr = ErrPrepareNeedleWriteBuffer
			return ErrPrepareNeedleWriteBuffer
		}
		// 1.3 get the write-offset
		var offset int64
		offset, _, err = scanner.dstDataBackend.GetStat()
		if err != nil {
			logrus.Errorf("failed to get write-offset, err: %v", err)
			scanner.ExitErr = ErrGetDataFileWriteOffset
			return ErrGetDataFileWriteOffset
		}
		// 1.4 write the bytes array into backend
		_, err = scanner.dstDataBackend.WriteAt(bytesToWrite, offset)
		if err != nil {
			logrus.Errorf("failed to write needle bytes, err: %v", err)
			scanner.ExitErr = ErrWriteNeedleBytes
			return ErrWriteNeedleBytes
		}

		// 2. write the needle index info to .idx file
		err = scanner.DstNeedleMap.Set(dstNeedle.Id, types.ToOffset(int64(offset)), dstNeedle.Size)
		if err != nil {
			logrus.Errorf("failed to set k/v for needle map, err: %v", err)
			scanner.ExitErr = ErrSetNeedleMap
			return ErrSetNeedleMap
		}
	}
	if !ok {
		logrus.Warningf("this needle <%d> seems to be deleted already", srcNeedle.Id)
	}
	return nil
}

func (scanner *VolumeFileScanner4Transformer) CreateDstNeedle(srcNeedle *needle.Needle) (dstNeedle *needle.Needle, err error) {
	dstNeedle = new(needle.Needle)
	// set Cookie + Id
	dstNeedle.Cookie = srcNeedle.Cookie
	dstNeedle.Id = srcNeedle.Id
	// set Data + DataSize
	dstNeedle.Data = make([]byte, srcNeedle.DataSize)
	copy(dstNeedle.Data, srcNeedle.Data)
	dstNeedle.DataSize = srcNeedle.DataSize
	// set Name + NameSize
	dstNeedle.Name = make([]byte, srcNeedle.NameSize)
	copy(dstNeedle.Name, srcNeedle.Name)
	dstNeedle.NameSize = srcNeedle.NameSize
	dstNeedle.SetHasName()
	// set Mime + MimeSize
	dstNeedle.Mime = make([]byte, srcNeedle.MimeSize)
	copy(dstNeedle.Mime, srcNeedle.Mime)
	dstNeedle.MimeSize = srcNeedle.MimeSize
	dstNeedle.SetHasMime()
	// set Pairs + PairsSize
	dstNeedle.Pairs = make([]byte, srcNeedle.PairsSize)
	copy(dstNeedle.Pairs, srcNeedle.Pairs)
	dstNeedle.PairsSize = srcNeedle.PairsSize
	dstNeedle.SetHasPairs()
	// set LastModified
	var timestamp int64
	timestamp, _ = scanner.FetchFakeTimestamp()
	dstNeedle.LastModified = uint64(timestamp)
	dstNeedle.SetHasLastModifiedDate()
	// set Ttl
	now := time.Now()
	storedDays := uint32(((now.UnixNano() - int64(srcNeedle.AppendAtNs)) / 1e9) / 86400)
	days := int(srcNeedle.Ttl.ToUint32()>>8 - storedDays)
	dstNeedle.Ttl, err = needle.ReadTTL(strconv.Itoa(days) + "d")
	if err != nil {
		return
	}
	if dstNeedle.Ttl != needle.EMPTY_TTL {
		dstNeedle.SetHasTtl()
	}
	// set Checksum
	dstNeedle.Checksum = needle.NewCRC(dstNeedle.Data)

	if srcNeedle.IsGzipped() {
		dstNeedle.SetGzipped()
	}

	if srcNeedle.IsChunkedManifest() {
		dstNeedle.SetIsChunkManifest()
	}
	return
}

func (scanner *VolumeFileScanner4Transformer) Counter() int64 {
	return scanner.counter
}

func (scanner *VolumeFileScanner4Transformer) ShouldBeDeleted() int64 {
	return scanner.shouldBeDeleted
}

func (scanner *VolumeFileScanner4Transformer) Close() {
	_ = scanner.dstDataBackend.Close()
}
