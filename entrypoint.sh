#!/bin/sh
set -e

CONFIG_PATH=/app/config.ini

# 如果 config.ini 是目录（Docker 挂载空目录导致），删除后重建文件
if [ -d "$CONFIG_PATH" ]; then
    echo "[info] config.ini 是目录，删除后自动生成文件..."
    rm -rf "$CONFIG_PATH"
fi

# 如果 config.ini 不存在，自动生成示例文件
if [ ! -f "$CONFIG_PATH" ]; then
    echo "[info] config.ini 不存在，自动生成示例文件..."
    cat > "$CONFIG_PATH" << 'EOF'
[Settings]
config_password = admin

[Task1]
name = 广东电信
type = udp
ip = 113.101.245.14
port = 9988
multicast = 239.77.0.112:5146
start_c = 245
end_c = 246
enabled = yes

[Task2]
name = 河南酒店
type = hls
base_ip = 222.89.96.36
start_c = 96
end_c = 96
port = 8001
path = /hls/2/index.m3u8
enabled = yes
EOF
fi

# 确保 data 目录存在
mkdir -p /app/data

# 执行传入的命令
exec "$@"
