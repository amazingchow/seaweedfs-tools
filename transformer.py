# -*- coding: utf-8 -*-
import argparse
import glob
import os
import ntpath
import shutil
import subprocess
import sys
from collections import defaultdict


if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--src", type=str, dest="src",
        help="directory to store volume data files, the .idx and .dat files should already exist inside the dir.")
    argparser.add_argument("--dst", type=str, dest="dst",
        help="directory to store encrypted volume data files.")
    args = argparser.parse_args()
    src_dir = args.src
    dst_dir = args.dst

    if src_dir != "" and dst_dir != "":
        if not os.path.isfile("./transformer"):
            print("please provide transformer execution binary")
            sys.exit(-1)
        if not os.access("./transformer", os.X_OK):
            print("please set execution permission for transformer")
            sys.exit(-1)

        if not os.path.isdir(dst_dir):
            # 新目录不存在则为之创建一个
            os.makedirs(dst_dir)

        # 获取所有的collection-vid关系对
        src_dat_files = glob.glob(os.path.join(src_dir, "*.dat"))
        src_dat_files.sort()
        collection_vid_map = defaultdict(list)
        for dat_file in src_dat_files:
            filename = ntpath.basename(dat_file)
            filename_without_suffix = os.path.splitext(filename)[0]
            parts = filename_without_suffix.split("_")
            collection, vid =  "_".join(parts[:len(parts)-1]), parts[len(parts)-1]
            collection_vid_map[collection].append(vid)
            collection_vid_map[collection].append(vid)

        for collection, vids in collection_vid_map.items():
            for vid in vids:
                print("/-------------------- {}_{}.dat command --------------------/".format(collection, vid))
                commands = [
                    "env ENCRYPTION_KEY=TEpSZVlpTURwRENuS0JkNXBGZzQzUT09 ./transformer",
                    "-verbose=true",
                    "-collection={}".format(collection),
                    "-vid={}".format(vid),
                    "-src={}".format(src_dir),
                    "-dst={}".format(dst_dir),
                ]
                print(" ".join(commands))
                print("/-------------------- {}_{}.dat result --------------------/".format(collection, vid))
                pipe_output = subprocess.check_output(" ".join(commands), shell=True)
                print(pipe_output.decode("utf-8"))

                src_idx_file = os.path.join(src_dir, "{}_{}.idx".format(collection, vid))
                src_dat_file = os.path.join(src_dir, "{}_{}.dat".format(collection, vid))
                dst_idx_file = os.path.join(dst_dir, "{}_{}.idx".format(collection, vid))
                dst_dat_file = os.path.join(dst_dir, "{}_{}.dat".format(collection, vid))
                # 用新索引文件替换旧索引文件
                shutil.move(dst_idx_file, src_idx_file)
                # 用新数据文件替换旧数据文件
                shutil.move(dst_dat_file, src_dat_file)
        
        # 删除临时目录
        os.rmdir(dst_dir)
