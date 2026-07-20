#!/bin/sh
set -e

# 确保 data 目录存在
mkdir -p /app/data

CONFIG_IN_DATA=/app/data/config.ini
CONFIG_LINK=/app/config.ini

# 如果 config.ini 是一个目录（Docker 挂载空目录导致），先删除
if [ -d "$CONFIG_LINK" ]; then
    echo "[info] 删除挂载导致的目录..."
    rmdir "$CONFIG_LINK" 2>/dev/null || rm -rf "$CONFIG_LINK" 2>/dev/null || true
fi
# 如果是挂载的文件但不是我们需要的，也删除
if [ -f "$CONFIG_LINK" ] && [ ! -L "$CONFIG_LINK" ]; then
    rm -f "$CONFIG_LINK"
fi

# 如果 data 里没有 config.ini，自动生成
if [ ! -f "$CONFIG_IN_DATA" ]; then
    echo "[info] 自动生成 config.ini 到 data 目录..."
    cat > "$CONFIG_IN_DATA" << 'EOF'
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

# 创建软链接，让 app.py 能读到 /app/config.ini
if [ ! -L "$CONFIG_LINK" ] && [ ! -f "$CONFIG_LINK" ]; then
    ln -s "$CONFIG_IN_DATA" "$CONFIG_LINK"
    echo "[info] 已链接 config.ini"
fi

# 执行传入的命令
exec "$@"
