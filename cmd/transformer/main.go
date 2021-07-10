package main

import "flag"

var (
	_Dir        = flag.String("dir", ".n", "Directory to store volume data files.")
	_Collection = flag.String("collection", "", "The volume collection name.")
	_VolumeId   = flag.Int("volumeId", -1, "A volume id. The volume .dat and .idx files should already exist inside the dir.")
	_Verbose    = flag.Bool("verbose", false, "set verbose output")
)

func main() {
	flag.Parse()
}
