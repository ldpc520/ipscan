# IPTV 流媒体代理 Docker 镜像
FROM python:3.11-slim

LABEL maintainer="iptv-proxy"
LABEL description="IPTV UDP/HLS 流媒体代理面板"

# 设置时区
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 创建非 root 用户运行应用
RUN useradd --create-home --shell /bin/bash appuser

WORKDIR /app

# 安装依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY --chown=appuser:appuser . .

# 创建数据目录并设置权限
RUN mkdir -p /app/data && chown -R appuser:appuser /app

# entrypoint 脚本需要可执行权限
RUN chmod +x /app/entrypoint.sh

# 切换到非 root 用户
USER appuser

# 暴露端口
EXPOSE 6603

# 环境变量（可在运行时覆盖）
ENV ENABLE_SCANNER=1
ENV SCAN_INTERVAL=900

# 健康检查
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:6603/health')" || exit 1

# 入口脚本：自动生成 config.ini
ENTRYPOINT ["/app/entrypoint.sh"]

# 启动命令：优先使用 gunicorn 生产模式，失败则回退到 Flask 开发服务器
CMD ["sh", "-c", "\
    pip install gunicorn -q 2>/dev/null && \
    exec gunicorn --bind 0.0.0.0:6603 --workers 2 --threads 4 --timeout 120 app:app \
    || exec python app.py"]
