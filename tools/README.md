### 生成大批量的假数据

1. 先使用``volume_server_batch_upload.py``脚本生成一大批真数据
2. 运行``time-server``, 用于生成假时间戳
3. ``generate-date-with-specified-last-modified-time``调用``time-server``接口来生成一批假数据
