package main

import (
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/sirupsen/logrus"
)

type Backup struct {
	Dir         string
	Master      string
	Replication string
}

func (bk *Backup) Do(collection string, volumeId uint32) error {
	util.LoadConfiguration("security", false)
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	vid := needle.VolumeId(volumeId)

	// find volume location, replication, ttl info
	lookup, err := operation.Lookup(bk.Master, vid.String())
	if err != nil {
		logrus.Errorf("failed to look up volume <%d>, err: %v", vid, err)
		return err
	}
	volumeServer := lookup.Locations[0].Url

	status, err := operation.GetVolumeSyncStatus(volumeServer, grpcDialOption, uint32(vid))
	if err != nil {
		logrus.Errorf("failed to get volume <%d> status, err: %v", vid, err)
		return err
	}

	ttl, err := needle.ReadTTL(status.Ttl)
	if err != nil {
		logrus.Errorf("failed to get volume <%d> ttl, err: %v", vid, err)
		return err
	}

	var replication *super_block.ReplicaPlacement
	if bk.Replication != "" {
		replication, err = super_block.NewReplicaPlacementFromString(bk.Replication)
		if err != nil {
			logrus.Errorf("failed to get volume <%d> replication, err: %v", vid, err)
			return err
		}
	} else {
		replication, err = super_block.NewReplicaPlacementFromString(status.Replication)
		if err != nil {
			logrus.Errorf("failed to get volume <%d> replication, err: %v", vid, err)
			return err
		}
	}

	volume, err := storage.NewVolume(bk.Dir, collection, vid, storage.NeedleMapInMemory, replication, ttl, 0, 0)
	if err != nil {
		logrus.Errorf("failed to create or read from volume <%d>, err: %v", vid, err)
		return err
	}

	if volume.SuperBlock.CompactionRevision < uint16(status.CompactRevision) {
		if err = volume.Compact2(30 * 1024 * 1024 * 1024); err != nil {
			logrus.Errorf("failed to compact volume before sync, err: %v", err)
			return err
		}
		if err = volume.CommitCompact(); err != nil {
			logrus.Errorf("failed to compact volume before sync, err: %v", err)
			return err
		}
		volume.SuperBlock.CompactionRevision = uint16(status.CompactRevision)
		volume.DataBackend.WriteAt(volume.SuperBlock.Bytes(), 0)
	}

	datSize, _, _ := volume.FileStat()

	if datSize > status.TailOffset {
		// remove the old data
		volume.Destroy()
		// recreate an empty volume
		volume, err = storage.NewVolume(bk.Dir, collection, vid, storage.NeedleMapInMemory, replication, ttl, 0, 0)
		if err != nil {
			logrus.Errorf("failed to create or read from volume <%d>, err: %v", vid, err)
			return err
		}
	}
	defer volume.Close()

	if err = volume.IncrementalBackup(volumeServer, grpcDialOption); err != nil {
		logrus.Errorf("failed to sync volume <%d>, err: %v", vid, err)
		return err
	}

	return nil
}
