"""
IPTV 扫描引擎模块
可被 app.py 作为后台线程调用，也可被 new_scan.py 独立运行。
"""
import asyncio
import configparser
import logging
import os
import time
from datetime import datetime
from typing import List, Optional

import aiohttp

# ==================== 日志 ====================
logger = logging.getLogger("scanner")

# ==================== 默认配置 ====================
DEFAULT_CONFIG_FILE = "config.ini"
DEFAULT_PID_FILE = "scanner.pid"
DEFAULT_SCAN_INTERVAL = 15 * 60
DEFAULT_TIMEOUT = 10
DEFAULT_MAX_CONCURRENT = 150
DEFAULT_PROGRESS_STEP = 300

# ==================== 工具 ====================

def create_default_config(config_file: str = DEFAULT_CONFIG_FILE):
    if not os.path.exists(config_file):
        config = configparser.ConfigParser()
        config["Task1"] = {
            "name": "广东电信",
            "type": "udp",
            "ip": "113.101.245.14",
            "port": "9988",
            "multicast": "239.77.0.1:5146",
            "start_c": "245",
            "end_c": "246",
            "enabled": "yes"
        }
        config["Task2"] = {
            "name": "河南酒店HLS",
            "type": "hls",
            "base_ip": "222.89.96.36",
            "start_c": "96",
            "end_c": "96",
            "port": "8001",
            "path": "/hls/2/index.m3u8",
            "enabled": "yes"
        }
        config["Task3"] = {
            "name": "湖北酒店HLS",
            "type": "hls",
            "base_ip": "221.232.199.15",
            "start_c": "196",
            "end_c": "199",
            "port": "7777",
            "path": "/tsfile/live/0002_1.m3u8?key=txiptv&playlive=1&authid=0",
            "enabled": "yes"
        }
        with open(config_file, "w", encoding="utf-8") as f:
            config.write(f)
        logger.info("已创建默认配置文件 %s", config_file)


def load_config(config_file: str = DEFAULT_CONFIG_FILE):
    config = configparser.ConfigParser()
    config.read(config_file, encoding="utf-8")
    return config


def gen_ip_list(base_ip: str, start_c: int, end_c: int) -> List[str]:
    prefix = '.'.join(base_ip.split('.')[:2]) + '.'
    return [f"{prefix}{c}.{d}" for c in range(start_c, end_c + 1) for d in range(256)]


# ==================== UDP ====================

async def check_udp(session, ip, port, multicast, timeout: int = DEFAULT_TIMEOUT) -> bool:
    try:
        async with session.get(
            f"http://{ip}:{port}/udp/{multicast}",
            timeout=aiohttp.ClientTimeout(total=timeout)
        ) as r:
            return r.status < 400
    except Exception:
        return False


async def scan_udp(task_id, info, exist_ip=None,
                   max_concurrent: int = DEFAULT_MAX_CONCURRENT,
                   timeout: int = DEFAULT_TIMEOUT,
                   progress_step: int = DEFAULT_PROGRESS_STEP):
    name = info["name"]
    ip = info["ip"]
    port = info["port"]
    multicast = info["multicast"]
    sc = int(info["start_c"])
    ec = int(info["end_c"])

    ips = gen_ip_list(ip, sc, ec)
    total = len(ips)

    logger.info(f"开始任务 {task_id}: {name}")
    logger.info(f"IP范围: {ip} C段 {sc}-{ec} | 端口: {port} | 组播: {multicast}")
    logger.info(f"待扫描IP总数: {total:,}")

    if exist_ip:
        logger.info(f"检查现有IP: {exist_ip}:{port}")
        async with aiohttp.ClientSession() as s:
            if await check_udp(s, exist_ip, port, multicast, timeout):
                logger.info(f"IP {exist_ip}:{port} 仍然有效，跳过扫描")
                return exist_ip
            logger.warning(f"IP {exist_ip}:{port} 已失效，重新扫描")

    found = None
    checked = 0
    start_time = time.time()
    semaphore = asyncio.Semaphore(max_concurrent)

    async def worker(session, ip):
        nonlocal found, checked
        async with semaphore:
            if found:
                return
            checked += 1
            if checked % progress_step == 0:
                elapsed = time.time() - start_time
                rate = checked / elapsed if elapsed > 0 else 0
                logger.info(f"进度: {checked:,}/{total:,} ({checked/total*100:.1f}%) | 速率: {rate:.1f}/s")
            if await check_udp(session, ip, port, multicast, timeout):
                found = ip
                logger.info(f"发现有效IP: {ip}:{port}")

    connector = aiohttp.TCPConnector(limit=max_concurrent, force_close=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [worker(session, ip) for ip in ips]
        for i in range(0, len(tasks), 2000):
            await asyncio.gather(*tasks[i:i + 2000])
            await asyncio.sleep(0.1)

    if not found:
        logger.warning(f"任务 {task_id} 未发现有效IP")

    return found


# ==================== HLS ====================

async def check_hls_real(session, base_url: str, timeout: int = DEFAULT_TIMEOUT) -> bool:
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        t = aiohttp.ClientTimeout(total=timeout)

        async with session.get(base_url, timeout=t, headers=headers) as r:
            if r.status != 200:
                return False
            m3u8_text = await r.text()

        if "#EXTM3U" not in m3u8_text:
            return False

        ts_url = None
        for line in m3u8_text.splitlines():
            line = line.strip()
            if line.startswith("#EXTINF"):
                continue
            if line and not line.startswith("#"):
                ts_url = line
                break

        if not ts_url:
            return False

        if ts_url.startswith("http"):
            pass
        elif ts_url.startswith("/"):
            from urllib.parse import urlparse
            parsed = urlparse(base_url)
            ts_url = f"{parsed.scheme}://{parsed.netloc}{ts_url}"
        else:
            ts_url = base_url.rsplit("/", 1)[0] + "/" + ts_url

        async with session.get(
            ts_url, timeout=t,
            headers={**headers, "Range": "bytes=0-65535"}
        ) as r:
            if r.status not in (200, 206):
                return False
            data = await r.read()
            return len(data) > 1000

    except Exception:
        return False


async def scan_hls(task_id, info, exist_ip=None,
                   max_concurrent: int = DEFAULT_MAX_CONCURRENT,
                   timeout: int = DEFAULT_TIMEOUT,
                   progress_step: int = DEFAULT_PROGRESS_STEP):
    name = info["name"]
    base_ip = info["base_ip"]
    sc = int(info["start_c"])
    ec = int(info["end_c"])
    port = int(info["port"])
    path = info["path"]

    ips = gen_ip_list(base_ip, sc, ec)
    total = len(ips)

    logger.info(f"开始任务 {task_id}: {name}")
    logger.info(f"IP范围: {base_ip} C段 {sc}-{ec} | 端口: {port}")
    logger.info(f"待扫描IP总数: {total:,}")

    if exist_ip:
        logger.info(f"检查现有IP: {exist_ip}:{port}")
        async with aiohttp.ClientSession() as s:
            if await check_hls_real(s, f"http://{exist_ip}:{port}{path}", timeout):
                logger.info(f"IP {exist_ip}:{port} 仍然有效，跳过扫描")
                return exist_ip
            logger.warning(f"IP {exist_ip}:{port} 已失效，重新扫描")

    found = None
    checked = 0
    start_time = time.time()
    semaphore = asyncio.Semaphore(max_concurrent)

    async def worker(session, ip):
        nonlocal found, checked
        async with semaphore:
            if found:
                return
            checked += 1
            if checked % progress_step == 0:
                elapsed = time.time() - start_time
                rate = checked / elapsed if elapsed > 0 else 0
                logger.info(f"进度: {checked:,}/{total:,} ({checked/total*100:.1f}%) | 速率: {rate:.1f}/s")
            url = f"http://{ip}:{port}{path}"
            if await check_hls_real(session, url, timeout):
                found = ip
                logger.info(f"发现有效直播源: {ip}:{port}")

    connector = aiohttp.TCPConnector(limit=max_concurrent, force_close=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [worker(session, ip) for ip in ips]
        for i in range(0, len(tasks), 2000):
            await asyncio.gather(*tasks[i:i + 2000])
            await asyncio.sleep(0.1)

    if not found:
        logger.warning(f"任务 {task_id} 未发现有效直播源")

    return found


# ==================== 保存 ====================

def save_udp_ip(task_id, ip, port, output_dir: str = "."):
    filepath = os.path.join(output_dir, f"udp_ip_{task_id}.txt")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(f"{ip}:{port}")
    logger.info(f"已保存IP到 {filepath}: {ip}:{port}")


def save_hls_ip(task_id, ip, port, output_dir: str = "."):
    filepath = os.path.join(output_dir, f"hls_ip_{task_id}.txt")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(f"{ip}:{port}")
    logger.info(f"已保存IP到 {filepath}: {ip}:{port}")


# ==================== 失败计数与历史IP管理 ====================

FAIL_COUNT_FILE = "fail_count.ini"
HISTORY_IP_FILE = "history_ip.txt"
MAX_FAIL_COUNT = 3


def load_fail_counts(fail_count_file: str = FAIL_COUNT_FILE):
    fc = configparser.ConfigParser()
    fc.read(fail_count_file, encoding="utf-8")
    return fc


def save_fail_counts(fc, fail_count_file: str = FAIL_COUNT_FILE):
    with open(fail_count_file, "w", encoding="utf-8") as f:
        fc.write(f)


def get_fail_count(fc, task_id):
    section = f"Task{task_id}"
    if fc.has_section(section):
        return fc.getint(section, "fail_count", fallback=0)
    return 0


def set_fail_count(fc, task_id, count):
    section = f"Task{task_id}"
    if not fc.has_section(section):
        fc.add_section(section)
    fc.set(section, "fail_count", str(count))


def reset_fail_count(fc, task_id, fail_count_file: str = FAIL_COUNT_FILE):
    section = f"Task{task_id}"
    if fc.has_section(section):
        fc.remove_section(section)
    save_fail_counts(fc, fail_count_file)


def save_to_history(ip_port, history_ip_file: str = HISTORY_IP_FILE):
    with open(history_ip_file, "a", encoding="utf-8") as f:
        f.write(f"{ip_port}\n")
    logger.info(f"已保存历史IP到 {history_ip_file}: {ip_port}")


def disable_task_in_config(task_id, config_file: str = DEFAULT_CONFIG_FILE):
    config = load_config(config_file)
    section = f"Task{task_id}"
    if config.has_section(section):
        config.set(section, "enabled", "no")
        with open(config_file, "w", encoding="utf-8") as f:
            config.write(f)
        logger.info(f"任务 {task_id} 已被禁用 (enabled=no)")


# ==================== 主扫描循环 ====================

async def scan_round(config_file: str = DEFAULT_CONFIG_FILE,
                     output_dir: str = ".",
                     fail_count_file: str = FAIL_COUNT_FILE,
                     history_ip_file: str = HISTORY_IP_FILE,
                     scan_interval: int = DEFAULT_SCAN_INTERVAL,
                     max_concurrent: int = DEFAULT_MAX_CONCURRENT,
                     timeout: int = DEFAULT_TIMEOUT,
                     progress_step: int = DEFAULT_PROGRESS_STEP,
                     max_fail_count: int = MAX_FAIL_COUNT):
    """
    执行一轮扫描（供外部调度使用）。
    返回本轮发现的有效 IP 数量。
    """
    config = load_config(config_file)
    enabled_tasks = [s for s in config.sections() if config[s].get("enabled", "no") == "yes"]

    found_count = 0

    for idx, section in enumerate(enabled_tasks, 1):
        task_id = section.replace("Task", "")
        info = config[section]

        logger.info(f"\n处理任务 {idx}/{len(enabled_tasks)}: {info['name']}")

        exist_ip = None
        if info["type"] == "udp":
            ip_file = os.path.join(output_dir, f"udp_ip_{task_id}.txt")
        else:
            ip_file = os.path.join(output_dir, f"hls_ip_{task_id}.txt")

        if os.path.exists(ip_file):
            with open(ip_file, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if content:
                    exist_ip = content.split(':')[0]
                    logger.info(f"检测到已保存IP: {content}")

        try:
            if info["type"] == "udp":
                result = await scan_udp(task_id, info, exist_ip,
                                        max_concurrent=max_concurrent,
                                        timeout=timeout,
                                        progress_step=progress_step)
            else:
                result = await scan_hls(task_id, info, exist_ip,
                                        max_concurrent=max_concurrent,
                                        timeout=timeout,
                                        progress_step=progress_step)

            if result:
                found_count += 1
                if info["type"] == "udp":
                    save_udp_ip(task_id, result, info["port"], output_dir)
                else:
                    save_hls_ip(task_id, result, info["port"], output_dir)
                fc = load_fail_counts(fail_count_file)
                reset_fail_count(fc, task_id, fail_count_file)
            else:
                fc = load_fail_counts(fail_count_file)
                fail_cnt = get_fail_count(fc, task_id) + 1
                set_fail_count(fc, task_id, fail_cnt)
                save_fail_counts(fc, fail_count_file)
                logger.warning(f"任务 {task_id} 扫描失败，连续失败次数: {fail_cnt}/{max_fail_count}")

                if fail_cnt >= max_fail_count:
                    if os.path.exists(ip_file):
                        with open(ip_file, 'r', encoding='utf-8') as f:
                            old_ip_port = f.read().strip()
                            if old_ip_port:
                                save_to_history(old_ip_port, history_ip_file)
                        os.remove(ip_file)
                        logger.info(f"已删除文件: {ip_file}")
                    disable_task_in_config(task_id, config_file)
                    reset_fail_count(fc, task_id, fail_count_file)
        except Exception as e:
            logger.error(f"任务 {task_id} 执行失败: {e}")

    return found_count


async def main_loop(config_file: str = DEFAULT_CONFIG_FILE,
                    output_dir: str = ".",
                    fail_count_file: str = FAIL_COUNT_FILE,
                    history_ip_file: str = HISTORY_IP_FILE,
                    scan_interval: int = DEFAULT_SCAN_INTERVAL,
                    max_concurrent: int = DEFAULT_MAX_CONCURRENT,
                    timeout: int = DEFAULT_TIMEOUT,
                    progress_step: int = DEFAULT_PROGRESS_STEP,
                    max_fail_count: int = MAX_FAIL_COUNT):
    """
    主循环：定时扫描（可被独立进程或后台线程调用）。
    """
    create_default_config(config_file)
    round_num = 0

    while True:
        round_num += 1
        logger.info("=" * 60)
        logger.info(f"第 {round_num} 轮扫描开始 | 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)

        await scan_round(
            config_file=config_file,
            output_dir=output_dir,
            fail_count_file=fail_count_file,
            history_ip_file=history_ip_file,
            scan_interval=scan_interval,
            max_concurrent=max_concurrent,
            timeout=timeout,
            progress_step=progress_step,
            max_fail_count=max_fail_count,
        )

        logger.info("=" * 60)
        logger.info(f"第 {round_num} 轮扫描完成，休眠 {scan_interval // 60} 分钟")
        logger.info("=" * 60)
        await asyncio.sleep(scan_interval)
