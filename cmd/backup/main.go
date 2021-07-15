package main

import (
	"context"
	param_parser "flag"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	myutils "github.com/amazingchow/seaweedfs-tools/pkg/utils"
)

var (
	_Dir = param_parser.String("dir",
		"/mnt/locals/seeweedfsvolume/volume0/volume",
		"directory to store volume data files, the .idx and .dat files should already exist inside the dir.")
	_Replication = param_parser.String("replica",
		"000",
		"seaweedfs volume server replication parameter")
	_SkipReadOnly = param_parser.Bool("read_only",
		false,
		"skip read-only volumes")
	_MasterHttp = param_parser.String("master_http",
		"localhost:9333",
		"seaweedfs master server http endpoint")
	_MasterGrpc = param_parser.String("master_grpc",
		"localhost:19333",
		"seaweedfs master server grpc endpoint")
	_Verbose = param_parser.Bool("verbose",
		false,
		"verbose")
)

func main() {
	param_parser.Parse()

	if *_Verbose {
		logrus.SetLevel(logrus.DebugLevel)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	// TODO: add more dial options
	conn, err := grpc.Dial(*_MasterGrpc, grpc.WithInsecure())
	if err != nil {
		logrus.Fatalf("failed to connect to %s, err: %v", *_MasterGrpc, err)
	}
	client := master_pb.NewSeaweedClient(conn)

	// fetch volume topology info
	resp, err := client.VolumeList(ctx, &master_pb.VolumeListRequest{})
	if err != nil {
		logrus.Fatalf("failed to list volume info from %s, err: %v", *_MasterGrpc, err)
	}

	// fetch collection + volume id pairs
	collectionMap := myutils.CollectVolumeInfo(resp.TopologyInfo, *_SkipReadOnly)

	bk := &Backup{
		Dir:         *_Dir,
		Master:      *_MasterHttp,
		Replication: *_Replication,
	}
	for collection, vids := range collectionMap {
		for _, vid := range vids {
			retries := 0
			operation := func() error {
				if err := bk.Do(collection, vid); err != nil {
					logrus.Warningf("failed to sync with master <%s>, retry=%d, err: %v", *_MasterHttp, retries, err)
					idxFileName := path.Join(*_Dir, collection+"_"+strconv.Itoa(int(vid))+".idx")
					datFileName := path.Join(*_Dir, collection+"_"+strconv.Itoa(int(vid))+".dat")
					logrus.Infof("delete %s and %s and pull again\n", idxFileName, datFileName)
					_ = os.Remove(idxFileName)
					_ = os.Remove(datFileName)
					retries++
					return err
				}
				return nil
			}
			notify := func(e error, t time.Duration) {
				if e != nil {
					logrus.Infof("will retry in %d secs", t.Seconds())
				}
			}
			err = backoff.RetryNotify(operation, NewBackoffConfig(), notify)
			if err != nil {
				logrus.Fatalf("failed to sync with master <%s>, err: %v", *_MasterHttp, err)
			}
		}
	}
}

func NewBackoffConfig() backoff.BackOff {
	bo := backoff.NewExponentialBackOff()
	retry := backoff.WithMaxRetries(bo, 5)
	retry.Reset()
	return retry
}
