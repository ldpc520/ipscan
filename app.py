"""
IPTV 流媒体代理 — Flask 集成版
启动: python app.py
格式: http://localhost:6603/udp/广东电信/239.77.0.1:5146
      http://localhost:6603/hls/河南酒店HLS/hls/2/index.m3u8
"""
from flask import Flask, Response, request, render_template_string, jsonify, redirect, session
import os
import re
import json
import configparser
import socket
import time
import hashlib
import urllib.request
import asyncio
import threading
import logging
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

app = Flask(__name__)

# ═══════════════════════════════════════════════════════
#  基础配置 — 所有路径自动基于运行目录
# ═══════════════════════════════════════════════════════
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, 'config.ini')
DATA_DIR = os.path.join(BASE_DIR, 'data')
os.makedirs(DATA_DIR, exist_ok=True)

# 扫描器集成配置
ENABLE_SCANNER = os.environ.get('ENABLE_SCANNER', '1') == '1'
SCAN_INTERVAL = int(os.environ.get('SCAN_INTERVAL', 15 * 60))

# 日志配置
SCANNER_LOG_FILE = os.path.join(BASE_DIR, 'scanner.log')
LOG_MAX_BYTES = 2 * 1024 * 1024  # 2MB

# 组播 ID 目录
M3U_DIR = os.path.join(BASE_DIR, '组播ID')

# Token 验证配置（远程 token 文件 URL）
REMOTE_TOKEN_URL = os.environ.get('REMOTE_TOKEN_URL', 'https://nav.sdyun.eu.org/token/ip_token.txt')

# 配置页面密码：优先取环境变量，否则从 config.ini 读取
def _get_config_password():
    env_pwd = os.environ.get('CONFIG_PASSWORD', '')
    if env_pwd:
        return env_pwd
    try:
        cfg = configparser.ConfigParser()
        cfg.read(CONFIG_FILE, encoding='utf-8')
        return cfg.get('Settings', 'config_password', fallback='admin')
    except Exception:
        return 'admin'

CONFIG_PASSWORD = _get_config_password()

# Flask session 密钥
SECRET_KEY = os.environ.get('SECRET_KEY', 'iptv-proxy-secret-key')
app.secret_key = SECRET_KEY

AUTH_FLAG_FILE = os.path.join(DATA_DIR, 'auth_verified.txt')       # 服务器认证标志：存在即已认证
AUTH_REMOTE_HASH_FILE = os.path.join(DATA_DIR, 'auth_remote_hash.txt')  # 认证时远程 token 的哈希
AUTH_LAST_CHECK_FILE = os.path.join(DATA_DIR, 'auth_last_check.txt')    # 上次远程校对时间戳
REMOTE_CHECK_INTERVAL = 6 * 3600  # 每6小时校对一次远程 token 是否变更


# ── 解析 config.ini ──
def parse_config(path):
    cfg = configparser.ConfigParser()
    cfg.read(path, encoding='utf-8')
    sections = []
    for sec in cfg.sections():
        info = dict(cfg.items(sec))
        info['_section'] = sec
        sections.append(info)
    return sections

# ── 收集所有节点 ──
def collect_nodes():
    udp_nodes = []
    hls_nodes = []
    for info in parse_config(CONFIG_FILE):
        t = info.get('type', '').lower()
        name = info.get('name', info['_section'])
        tid = re.sub(r'^Task', '', info['_section'])
        if t == 'udp':
            f = os.path.join(BASE_DIR, f'udp_ip_{tid}.txt')
            if os.path.exists(f):
                c = open(f, encoding='utf-8').read().strip()
                if c:
                    parts = c.split(':', 1)
                    udp_nodes.append({
                        'name': name, 'ip_port': c,
                        'ip': parts[0], 'port': parts[1] if len(parts) > 1 else '',
                        'multicast': info.get('multicast', ''),
                    })
        elif t == 'hls':
            f = os.path.join(BASE_DIR, f'hls_ip_{tid}.txt')
            if os.path.exists(f):
                c = open(f, encoding='utf-8').read().strip()
                if c:
                    parts = c.split(':', 1)
                    hls_nodes.append({
                        'name': name, 'ip_port': c,
                        'ip': parts[0], 'port': parts[1] if len(parts) > 1 else '',
                        'path': info.get('path', '').lstrip('/'),
                    })
    return udp_nodes, hls_nodes

def get_groups(nodes):
    """将节点按 name 分组，返回分组列表（保持 config.ini 出现顺序）"""
    groups = []
    seen = set()
    for n in nodes:
        g = n['name']
        if g not in seen:
            seen.add(g)
            groups.append(g)
    return groups

def get_hls_groups(hls_nodes):
    """HLS: 按 path 去重后编号，相同 path 的归到同一组（酒店1、酒店2...）"""
    path_order = []
    seen = set()
    for n in hls_nodes:
        p = n['path']
        if p not in seen:
            seen.add(p)
            path_order.append(p)
    groups = []
    for i, p in enumerate(path_order):
        gname = f'酒店{i+1}'
        groups.append(gname)
    return groups, path_order

# ── TCP 测速：连接指定 IP:Port 并返回延迟(ms) ──
def tcp_ping(ip, port, timeout=3):
    """TCP 端口连通性测试，返回延迟(ms)，超时/失败返回 -1"""
    port = int(port) if port else 80
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        start = time.time()
        sock.connect((ip, port))
        elapsed = (time.time() - start) * 1000
        sock.close()
        return round(elapsed, 1)
    except Exception:
        return -1

# ── 选中节点持久化 ──
def _safe_filename(name):
    """将分组名转为安全的文件名"""
    return re.sub(r'[\\/:*?"<>|]', '_', name)

def get_selected_group(t, group):
    """读取某个分组选中的节点 ip:port"""
    f = os.path.join(DATA_DIR, f'{t}_{_safe_filename(group)}.txt')
    if not os.path.exists(f):
        return None
    s = open(f, encoding='utf-8').read().strip()
    if s and re.match(r'^\d{1,3}(\.\d{1,3}){3}:\d{1,5}$', s):
        return s
    return None

def get_selected_group_info(t, group):
    """读取某个分组选中的节点详细信息"""
    f = os.path.join(DATA_DIR, f'{t}_{_safe_filename(group)}_info.json')
    if os.path.exists(f):
        return json.load(open(f, encoding='utf-8'))
    return None

def get_all_selected(t):
    """获取某类型下所有已选分组: {group: ip_port}"""
    result = {}
    if not os.path.exists(DATA_DIR):
        return result
    prefix = f'{t}_'
    for fn in os.listdir(DATA_DIR):
        if fn.startswith(prefix) and fn.endswith('.txt') and '_info' not in fn:
            group_encoded = fn[len(prefix):-4]
            f = os.path.join(DATA_DIR, fn)
            s = open(f, encoding='utf-8').read().strip()
            if s and re.match(r'^\d{1,3}(\.\d{1,3}){3}:\d{1,5}$', s):
                # 还原分组名：查找匹配的原始 group
                for info_fn in os.listdir(DATA_DIR):
                    if info_fn.startswith(prefix + group_encoded) and info_fn.endswith('_info.json'):
                        jf = os.path.join(DATA_DIR, info_fn)
                        try:
                            info = json.load(open(jf, encoding='utf-8'))
                            result[info.get('group', group_encoded)] = s
                        except Exception:
                            result[group_encoded] = s
                        break
                else:
                    result[group_encoded] = s
    return result

def get_all_selected_info(t):
    """获取某类型下所有已选分组的详细信息: {group: info}"""
    result = {}
    if not os.path.exists(DATA_DIR):
        return result
    prefix = f'{t}_'
    for fn in os.listdir(DATA_DIR):
        if fn.startswith(prefix) and fn.endswith('_info.json'):
            try:
                info = json.load(open(os.path.join(DATA_DIR, fn), encoding='utf-8'))
                group = info.get('group', '')
                if group:
                    result[group] = info
            except Exception:
                pass
    return result


# ── Token 验证（服务器级别认证）──
# 设计思路：服务器是"公共入口"，只要任意设备认证过一次，整个服务器即"解锁"，
# 后续所有设备访问都不再需要输入 token。直到远程 token 变更才会要求重新认证。
# 每 6 小时向远程校对一次 token 是否变更。

def _token_hash(raw):
    if not raw:
        return ''
    return hashlib.sha256(raw.strip().encode()).hexdigest()


def fetch_remote_token():
    """从远程获取 token 内容，返回纯文本 token，失败返回 None"""
    if not REMOTE_TOKEN_URL:
        return None
    try:
        req = urllib.request.Request(REMOTE_TOKEN_URL, headers={
            'User-Agent': 'IPTV-Proxy/1.0'
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            content = resp.read().decode('utf-8', errors='replace').strip()
            if content:
                for line in content.splitlines():
                    line = line.strip()
                    if line:
                        return line
    except Exception as e:
        print(f'[Token] 获取远程 token 失败: {e}')
        return None


def _remote_token_changed():
    """检查远程 token 是否已变更（带 6 小时间隔缓存，避免频繁请求远端）
    返回 True 表示远程 token 与认证时不一致（已变更）
    """
    if not os.path.exists(AUTH_REMOTE_HASH_FILE):
        return False  # 从未认证过，不算变更

    # 有上次检查时间且在间隔内 → 直接信任缓存
    if os.path.exists(AUTH_LAST_CHECK_FILE):
        try:
            last_check = float(open(AUTH_LAST_CHECK_FILE, encoding='utf-8').read().strip())
            if time.time() - last_check < REMOTE_CHECK_INTERVAL:
                return False
        except Exception:
            pass

    # 超出间隔 → 从远程获取对比
    remote = fetch_remote_token()
    if remote is None:
        return False  # 获取失败，保守处理：不踢人

    remote_h = _token_hash(remote)
    _update_last_check()

    stored_h = open(AUTH_REMOTE_HASH_FILE, encoding='utf-8').read().strip()
    if remote_h == stored_h:
        return False   # 未变更
    else:
        print('[Token] 检测到远程 token 已变更')
        return True    # 已变更


def _update_last_check():
    with open(AUTH_LAST_CHECK_FILE, 'w', encoding='utf-8') as f:
        f.write(str(time.time()))


def _clear_auth_state():
    """清除所有服务器认证状态（远程 token 变更时调用）"""
    for fpath in [AUTH_FLAG_FILE, AUTH_REMOTE_HASH_FILE, AUTH_LAST_CHECK_FILE]:
        if os.path.exists(fpath):
            os.remove(fpath)
    print('[Token] 远程 token 已变更，服务器已锁定，等待重新认证')


def _is_server_authenticated():
    """服务器是否处于"已认证"状态（供 before_request 快速判断）"""
    if os.path.exists(AUTH_FLAG_FILE):
        # 已认证 → 检查远程 token 是否变更（每 6 小时一次）
        if not _remote_token_changed():
            return True   # 远程未变，服务器保持解锁
        else:
            _clear_auth_state()
            return False  # 远程已变，锁定服务器
    return False


def _set_auth_verified():
    """标记服务器为"已认证"状态"""
    with open(AUTH_FLAG_FILE, 'w', encoding='utf-8') as f:
        f.write('1')

@app.before_request
def check_token():
    """Token 验证（服务器级别）：只要服务器已认证，所有设备均可访问"""
    path = request.path
    # 放行：auth、config、代理路由、静态资源
    if path in ('/auth', '/config', '/api/logout'):
        return None
    if path.startswith('/udp/') or path.startswith('/hls/'):
        return None
    if path.startswith('/static'):
        return None
    if not REMOTE_TOKEN_URL:
        return None
    # 服务器已认证 → 放行所有设备
    if _is_server_authenticated():
        return None
    # 未认证 → 跳转认证页
    if path.startswith('/api/'):
        return jsonify(ok=False, error='auth_required'), 401
    return redirect(f'/auth?redirect={request.path}')


# ═══════════════════════════════════════════════════════
#  首页 — 自动跳转面板
# ═══════════════════════════════════════════════════════
@app.route('/health')
def health():
    """Docker / K8s 健康检查端点"""
    return jsonify(ok=True, status='running'), 200

@app.route('/')
def home():
    return redirect('/panel')

# ═══════════════════════════════════════════════════════
#  日志查看页
# ═══════════════════════════════════════════════════════
@app.route('/log')
def view_log():
    """Web 页面实时查看扫描日志，最新日志在最上面"""
    lines = 200
    try:
        n = int(request.args.get('lines', 200))
        lines = max(10, min(n, 2000))
    except Exception:
        pass

    log_text = ''
    if os.path.exists(SCANNER_LOG_FILE):
        try:
            with open(SCANNER_LOG_FILE, 'r', encoding='utf-8') as f:
                all_lines = f.readlines()
                log_text = ''.join(all_lines[-lines:])
        except Exception:
            log_text = '读取日志失败'

    return render_template_string(LOG_HTML,
        log_text=log_text, lines=lines,
        log_file=SCANNER_LOG_FILE,
        log_size=os.path.getsize(SCANNER_LOG_FILE) if os.path.exists(SCANNER_LOG_FILE) else 0,
        host=request.host)

# ═══════════════════════════════════════════════════════
#  面板页
# ═══════════════════════════════════════════════════════
@app.route('/panel')
def panel():
    udp_nodes, hls_nodes = collect_nodes()
    udp_groups = get_groups(udp_nodes)
    hls_groups = get_groups(hls_nodes)
    # 给每个 HLS 节点标记其所属分组
    for n in hls_nodes:
        n['group'] = n['name']
    sel_udp = get_all_selected('udp')
    sel_hls = get_all_selected('hls')
    sel_info_udp = get_all_selected_info('udp')
    sel_info_hls = get_all_selected_info('hls')

    return render_template_string(PANEL_HTML,
        udp_nodes=udp_nodes, hls_nodes=hls_nodes,
        udp_groups=udp_groups, hls_groups=hls_groups,
        sel_udp=sel_udp, sel_hls=sel_hls,
        sel_info_udp=sel_info_udp, sel_info_hls=sel_info_hls,
        host=request.host)

# ═══════════════════════════════════════════════════════
#  整套 ID 页面（按分组显示组播ID，ip:prot 替换为代理地址）
# ═══════════════════════════════════════════════════════
@app.route('/ids/<group>')
def group_ids(group):
    """读取 组播ID/分组名.txt，把 ip:prot 替换为当前代理地址后展示"""
    file_path = os.path.join(M3U_DIR, f'{group}.txt')
    if not os.path.exists(file_path):
        return render_template_string(IDS_HTML,
            group=group, lines=[], error='未找到该分组 ID 文件', host=request.host)

    proxy_prefix = f'http://{request.host}'
    lines = []
    with open(file_path, encoding='utf-8') as f:
        for line in f:
            line = line.rstrip('\n')
            if not line.strip():
                continue
            # 替换 http://ip:prot/udp/ 为 http://host/udp/分组名/
            new_line = re.sub(
                r'http://ip:prot/udp/',
                f'{proxy_prefix}/udp/{group}/',
                line
            )
            # 替换 http://ip:prot/ 为 http://host/（通用兜底）
            new_line = new_line.replace('http://ip:prot/', f'{proxy_prefix}/')
            lines.append(new_line)

    return render_template_string(IDS_HTML,
        group=group, lines=lines, error='', host=request.host)

# ═══════════════════════════════════════════════════════
#  认证页面
# ═══════════════════════════════════════════════════════
@app.route('/auth', methods=['GET', 'POST'])
def auth():
    error = ''
    if request.method == 'POST':
        raw = request.form.get('token', '').strip()
        if not REMOTE_TOKEN_URL:
            _set_auth_verified()
            return redirect(request.args.get('redirect', '/panel'))
        remote = fetch_remote_token()
        if remote is None:
            error = '无法连接验证服务器，请稍后重试'
        elif raw == remote:
            # 认证通过 → 标记服务器已解锁 + 记录远程 token 哈希
            _set_auth_verified()
            with open(AUTH_REMOTE_HASH_FILE, 'w', encoding='utf-8') as f:
                f.write(_token_hash(remote))
            _update_last_check()
            print('[Token] 服务器认证已激活，所有设备均可访问')
            return redirect(request.args.get('redirect', '/panel'))
        else:
            error = 'Token 无效，请重试'
    return render_template_string(AUTH_HTML,
        error=error, redirect=request.args.get('redirect', '/panel'),
        host=request.host)

# ═══════════════════════════════════════════════════════
#  配置管理页面
# ═══════════════════════════════════════════════════════
@app.route('/config', methods=['GET', 'POST'])
def config_page():
    """配置编辑：需要密码登录（密码从 config.ini [Settings] 读取）"""
    logged = session.get('config_authed', False)
    error = ''

    if request.method == 'POST':
        action = request.form.get('action', '')

        # 登录
        if action == 'login':
            pwd = request.form.get('password', '').strip()
            if pwd == CONFIG_PASSWORD:
                session['config_authed'] = True
                logged = True
            else:
                error = '密码错误'

        # 保存配置
        elif action == 'save' and logged:
            new_content = request.form.get('config_content', '')
            try:
                with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                    f.write(new_content.strip() + '\n')
                return render_template_string(CONFIG_HTML,
                    logged=True, error='配置已保存成功 ✓',
                    config_content=new_content.strip(),
                    host=request.host)
            except Exception as e:
                error = f'保存失败: {e}'

        # 注销
        elif action == 'logout':
            session.pop('config_authed', None)
            logged = False

    config_content = ''
    if logged and os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, encoding='utf-8') as f:
            config_content = f.read()

    return render_template_string(CONFIG_HTML,
        logged=logged, error=error,
        config_content=config_content,
        host=request.host)

@app.route('/api/logout')
def api_logout():
    session.clear()
    return redirect('/auth')

# ── API: 选择节点（按分组存储） ──
@app.route('/api/select')
def api_select():
    t = request.args.get('type', '')
    ip_port = request.args.get('ip_port', '')
    node_id = request.args.get('id', '')
    group = request.args.get('group', '')
    if t not in ('udp', 'hls'):
        return jsonify(ok=False, error='invalid type'), 400
    if not re.match(r'^\d{1,3}(\.\d{1,3}){3}:\d{1,5}$', ip_port):
        return jsonify(ok=False, error='invalid ip_port'), 400

    gsafe = _safe_filename(group)
    with open(os.path.join(DATA_DIR, f'{t}_{gsafe}.txt'), 'w', encoding='utf-8') as f:
        f.write(ip_port)
    with open(os.path.join(DATA_DIR, f'{t}_{gsafe}_info.json'), 'w', encoding='utf-8') as f:
        json.dump({'ip_port': ip_port, 'id': node_id, 'group': group,
                    'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')},
                   f, ensure_ascii=False)
    return jsonify(ok=True, type=t, group=group, ip_port=ip_port, id=node_id)

# ── API: 清除（按分组清除） ──
@app.route('/api/clear')
def api_clear():
    t = request.args.get('type', '')
    group = request.args.get('group', '')
    if t not in ('udp', 'hls'):
        return jsonify(ok=False, error='invalid type'), 400
    gsafe = _safe_filename(group)
    for ext in ('.txt', '_info.json'):
        p = os.path.join(DATA_DIR, f'{t}_{gsafe}{ext}')
        if os.path.exists(p):
            os.remove(p)
    return jsonify(ok=True, type=t, group=group)

# ── API: 节点测速（单个） ──
@app.route('/api/speedtest')
def api_speedtest():
    t = request.args.get('type', '')
    ip_port = request.args.get('ip_port', '')
    if t not in ('udp', 'hls'):
        return jsonify(ok=False, error='invalid type'), 400
    if not ip_port or ':' not in ip_port:
        return jsonify(ok=False, error='invalid ip_port'), 400
    ip, port = ip_port.split(':', 1)
    latency = tcp_ping(ip, port)
    if latency < 0:
        return jsonify(ok=True, ip_port=ip_port, latency=-1, text='超时')
    else:
        return jsonify(ok=True, ip_port=ip_port, latency=latency, text=f'{latency}ms')

# ── API: 批量并发测速 ──
@app.route('/api/speedtest_batch', methods=['POST'])
def api_speedtest_batch():
    """接收 ip_port 列表，并发测速后返回结果"""
    data = request.get_json(force=True)
    ip_ports = data.get('ip_ports', [])
    if not ip_ports:
        return jsonify(ok=False, error='empty list'), 400

    results_map = {}
    with ThreadPoolExecutor(max_workers=min(len(ip_ports), 20)) as executor:
        futures = {}
        for ip_port in ip_ports:
            if ':' in ip_port:
                ip, port = ip_port.split(':', 1)
                futures[executor.submit(tcp_ping, ip, port)] = ip_port

        for future in as_completed(futures):
            ip_port = futures[future]
            try:
                latency = future.result()
            except Exception:
                latency = -1
            results_map[ip_port] = {
                'ip_port': ip_port,
                'latency': latency,
                'text': f'{latency}ms' if latency >= 0 else '超时'
            }

    # 按原始顺序返回
    results = [results_map.get(ip, {'ip_port': ip, 'latency': -1, 'text': '超时'}) for ip in ip_ports]
    return jsonify(ok=True, results=results)

# ═══════════════════════════════════════════════════════
#  ★ 核心：流式代理
#    UDP: /udp/<group>/<channel_id>  (分组名区分相同multicast)
#    HLS: /hls/<group>/<path>       (分组名区分相同path)
# ═══════════════════════════════════════════════════════
@app.route('/udp/<group>/<path:channel_id>')
def proxy_udp(group, channel_id):
    """UDP: 根据分组名直接定位节点"""
    server = get_selected_group('udp', group)
    if not server:
        return Response(
            f'<html><body style="font-family:sans-serif;text-align:center;padding:60px;'
            f'background:#0f0f14;color:#c8c8d4">'
            f'<h2 style="color:#ef4444">未选择 [{group}] UDP 节点</h2>'
            f'<p>请先访问 <a style="color:#6366f1" href="/panel">面板</a> 选择节点</p>'
            f'</body></html>',
            status=503, content_type='text/html; charset=utf-8')

    channel_id = channel_id.lstrip('/')
    upstream = f'http://{server}/udp/{channel_id}'
    print(f'[{datetime.now().strftime("%H:%M:%S")}] [{group}] UDP 302 -> {upstream}')
    return redirect(upstream, code=302)

@app.route('/hls/<group>/<path:channel_id>')
def proxy_hls(group, channel_id):
    """HLS: 根据分组名直接定位节点"""
    server = get_selected_group('hls', group)
    if not server:
        return Response(
            f'<html><body style="font-family:sans-serif;text-align:center;padding:60px;'
            f'background:#0f0f14;color:#c8c8d4">'
            f'<h2 style="color:#ef4444">未选择 [{group}] HLS 节点</h2>'
            f'<p>请先访问 <a style="color:#6366f1" href="/panel">面板</a> 选择节点</p>'
            f'</body></html>',
            status=503, content_type='text/html; charset=utf-8')

    channel_id = channel_id.lstrip('/')
    upstream = f'http://{server}/{channel_id}'
    print(f'[{datetime.now().strftime("%H:%M:%S")}] [{group}] HLS 302 -> {upstream}')
    return redirect(upstream, code=302)

# ═══════════════════════════════════════════════════════
#  HTML 模板
# ═══════════════════════════════════════════════════════

AUTH_HTML = r'''
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>认证 - IPTV Proxy</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Segoe UI',sans-serif;background:#0f0f14;color:#c8c8d4;
display:flex;justify-content:center;align-items:center;min-height:100vh}
.card{background:#1a1a24;border-radius:16px;padding:40px;width:100%;max-width:380px;
border:1px solid #2a2a3a;text-align:center}
h1{font-size:22px;margin-bottom:24px;color:#e0e0ea}
input{width:100%;padding:12px 16px;border-radius:10px;border:1px solid #333;
background:#0f0f14;color:#e0e0ea;font-size:15px;outline:none;text-align:center}
input:focus{border-color:#6366f1}
.btn{width:100%;padding:12px;margin-top:14px;border-radius:10px;border:none;
font-size:15px;cursor:pointer;font-weight:600;transition:all .2s}
.btn-primary{background:#6366f1;color:#fff}
.btn-primary:hover{background:#7c3aed}
.error{color:#ef4444;font-size:13px;margin-top:12px}
.hint{font-size:12px;color:#666;margin-top:18px}
</style>
</head>
<body>
<div class="card">
<h1>🔒 Token 验证</h1>
<form method="post">
<input type="text" name="token" placeholder="请输入访问 Token" autofocus required>
<button class="btn btn-primary" type="submit">验证登录</button>
{% if error %}
<p class="error">{{ error }}</p>
{% endif %}
</form>
<p class="hint">请粘贴有效的访问 Token 进行验证</p>
</div>
</body>
</html>
'''

CONFIG_HTML = r'''
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>配置管理 - IPTV Proxy</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Segoe UI',sans-serif;background:#0f0f14;color:#c8c8d4}
.container{max-width:860px;margin:0 auto;padding:30px 20px}
.header{display:flex;justify-content:space-between;align-items:center;margin-bottom:20px}
h1{font-size:24px;color:#e0e0ea}
.links a{color:#6366f1;text-decoration:none;margin-left:20px;font-size:14px}
.links a:hover{text-decoration:underline}
.card{background:#1a1a24;border-radius:16px;padding:30px;border:1px solid #2a2a3a}
input[type=password]{width:100%;padding:12px 16px;border-radius:10px;border:1px solid #333;
background:#0f0f14;color:#e0e0ea;font-size:15px;outline:none;margin-bottom:12px}
input[type=password]:focus{border-color:#6366f1}
textarea{width:100%;height:500px;background:#0f0f14;color:#e0e0ea;border:1px solid #333;
border-radius:10px;padding:16px;font-family:'Cascadia Code','Consolas',monospace;
font-size:13px;line-height:1.6;resize:vertical;outline:none}
textarea:focus{border-color:#6366f1}
.btn{padding:10px 24px;border-radius:10px;border:none;font-size:14px;cursor:pointer;
font-weight:600;transition:all .2s}
.btn-primary{background:#6366f1;color:#fff}
.btn-primary:hover{background:#7c3aed}
.btn-danger{background:#3a3a4a;color:#c8c8d4}
.btn-danger:hover{background:#4a4a5a}
.btn-group{display:flex;gap:10px;margin-top:14px}
.msg{font-size:14px;margin-top:12px;padding:10px 16px;border-radius:8px}
.msg-success{background:#064e3b;color:#6ee7b7}
.msg-error{background:#7f1d1d;color:#fca5a5}
.back{margin-bottom:16px}
.back a{color:#6366f1;text-decoration:none;font-size:14px}
.back a:hover{text-decoration:underline}
</style>
</head>
<body>
<div class="container">
<div class="header">
<h1>⚙️ 配置管理</h1>
<div class="links">
<a href="/panel">面板</a>
{% if logged %}
<a href="/config" onclick="event.preventDefault();document.getElementById('logoutForm').submit()">注销</a>
{% endif %}
</div>
</div>

{% if not logged %}
<div class="card">
<h2 style="font-size:18px;margin-bottom:16px;color:#e0e0ea">请输入管理密码</h2>
<form method="post">
<input type="hidden" name="action" value="login">
<input type="password" name="password" placeholder="管理密码" autofocus required>
<button class="btn btn-primary" type="submit">登录</button>
{% if error %}
<p class="msg msg-error">{{ error }}</p>
{% endif %}
</form>
<p style="font-size:12px;color:#666;margin-top:16px">
密码在 <code>config.ini</code> 的 <code>[Settings]</code> 段中配置</p>
</div>
{% else %}
<div class="back"><a href="/panel">← 返回面板</a></div>
<div class="card">
{% if error %}
<p class="msg {% if '成功' in error %}msg-success{% else %}msg-error{% endif %}">{{ error }}</p>
{% endif %}
<form method="post">
<input type="hidden" name="action" value="save">
<textarea name="config_content" spellcheck="false">{{ config_content }}</textarea>
<div class="btn-group">
<button class="btn btn-primary" type="submit">💾 保存配置</button>
</div>
</form>
<form id="logoutForm" method="post" style="display:none">
<input type="hidden" name="action" value="logout">
</form>
</div>
{% endif %}
</div>
</body>
</html>
'''

LOG_HTML = r'''
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<title>扫描日志</title>
<style>
:root {--bg:#0f0f14;--card:#1a1a24;--border:#2a2a3a;--text:#c8c8d4;--dim:#6a6a7a;--accent:#6366f1;--warn:#eab308;--err:#ef4444;--ok:#22c55e}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Consolas','Courier New',monospace;background:var(--bg);color:var(--text);min-height:100vh;overflow-x:hidden}
/* ── 顶部栏 ── */
.top{background:var(--card);border-bottom:1px solid var(--border);padding:10px 20px;display:flex;align-items:center;gap:14px;position:sticky;top:0;z-index:10;backdrop-filter:blur(8px)}
.top h1{font-size:15px;color:#e0e0e8;white-space:nowrap;display:flex;align-items:center;gap:6px}
.top h1 .icon{font-size:17px}
.top .info{font-size:11px;color:var(--dim);white-space:nowrap}
.top .spacer{flex:1;min-width:0}
.top a,.top button{font-size:12px;padding:6px 16px;border-radius:6px;border:1px solid var(--border);background:transparent;color:var(--dim);cursor:pointer;text-decoration:none;transition:.15s;white-space:nowrap;line-height:1.4}
.top a:hover,.top button:hover{border-color:var(--accent);color:var(--accent)}
.top .refresh:hover{border-color:var(--ok);color:var(--ok)}
.top .back-btn{border-color:var(--accent);color:var(--accent)}
.top .back-btn:hover{background:var(--accent);color:#fff}
/* ── 内容区 ── */
.log-wrap{max-width:1200px;margin:0 auto;padding:12px 20px 40px}
.log-line{font-size:13px;line-height:1.8;white-space:pre-wrap;word-break:break-all;padding:2px 8px;border-radius:3px;margin:1px 0;transition:background .1s}
.log-line:hover{background:rgba(255,255,255,.03)}
.log-line.info{color:var(--text)}
.log-line.warn{color:var(--warn);background:rgba(234,179,8,.05)}
.log-line.err{color:var(--err);background:rgba(239,68,68,.05)}
.empty{text-align:center;padding:80px 20px;color:var(--dim);font-size:14px}
.empty .icon{font-size:40px;display:block;margin-bottom:12px;opacity:.3}
/* ── 移动端：顶部栏折叠为两行 ── */
@media(max-width:768px){
  .top{flex-wrap:wrap;padding:10px 14px;gap:8px}
  .top h1{font-size:14px;width:100%}
  .top .info{font-size:10px;order:10}
  .top .auto-row{order:20;display:flex;align-items:center;gap:8px;width:100%}
  .top .auto-refresh{font-size:11px}
  .top .spacer{display:none}
  .top a,.top button{font-size:11px;padding:5px 12px;border-radius:5px}
  .log-wrap{padding:8px 10px 30px;max-width:100%}
  .log-line{font-size:11px;line-height:1.7;padding:2px 6px}
  .empty{padding:60px 16px;font-size:13px}
}
@media(max-width:400px){
  .top{padding:8px 10px;gap:6px}
  .top h1{font-size:13px}
  .top .info{font-size:9px}
  .top a,.top button{font-size:10px;padding:4px 10px}
  .log-wrap{padding:6px 6px 24px}
  .log-line{font-size:10px;line-height:1.6;padding:1px 4px}
}
</style>
</head>
<body>
<div class="top">
  <h1><span class="icon">📋</span>扫描日志</h1>
  <span class="info">{{ "%.1f"|format(log_size/1024) if log_size else 0 }} KB</span>
  <span class="info">{{ lines }} 行</span>
  <span class="spacer"></span>
  <div class="auto-row">
    <label class="auto-refresh"><input type="checkbox" id="auto" checked onchange="toggleAuto()"> 自动刷新</label>
    <button class="refresh" onclick="location.reload()">刷新</button>
    <a class="back-btn" href="/panel">← 面板</a>
  </div>
</div>
<div class="log-wrap">
{% if log_text %}
{% for line in log_text.split('\n') %}
{% set cls = 'err' if 'ERROR' in line or 'CRITICAL' in line else ('warn' if 'WARNING' in line else 'info') %}
<div class="log-line {{ cls }}">{{ line }}</div>
{% endfor %}
{% else %}
<div class="empty"><span class="icon">📭</span>暂无日志</div>
{% endif %}
</div>
<script>
var auto = true;
function toggleAuto(){auto = document.getElementById('auto').checked}
setInterval(function(){if(auto) location.reload()}, 5000);
window.scrollTo(0, document.body.scrollHeight);
</script>
</body>
</html>
'''

IDS_HTML = r'''
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>整套 ID - {{ group }}</title>
<style>
:root{--bg:#0f0f14;--card:#1a1a24;--border:#2a2a3a;--accent:#6366f1;--accent2:#22c55e;--text:#e0e0e8;--dim:#8b8b9a;--danger:#ef4444}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:var(--bg);color:var(--text);min-height:100vh}
.ct{max-width:860px;margin:0 auto;padding:16px 12px 40px}
h1{font-size:18px;text-align:center;margin-bottom:6px;color:#e0e0e8}
h1 span{color:var(--accent)}
.sub{text-align:center;font-size:12px;color:var(--dim);margin-bottom:16px}
.back{display:inline-flex;margin-bottom:14px;font-size:13px;color:var(--accent);text-decoration:none}
.back:hover{text-decoration:underline}
.card{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:14px;margin-bottom:14px}
.actions{display:flex;gap:10px;justify-content:flex-end;margin-bottom:12px}
.btn{font-size:12px;padding:6px 14px;border-radius:6px;border:1px solid var(--border);background:transparent;color:var(--dim);cursor:pointer;transition:.15s}
.btn:hover{border-color:var(--accent);color:var(--accent)}
.btn-primary{border-color:var(--accent);background:var(--accent);color:#fff}
.btn-primary:hover{background:#7c3aed}
.count{font-size:12px;color:var(--dim);margin-bottom:8px}
.error{color:var(--danger);font-size:13px;text-align:center;padding:30px}
pre{font-family:'Cascadia Code','Consolas',monospace;font-size:12px;line-height:1.8;white-space:pre-wrap;word-break:break-all;color:var(--text)}
pre a{color:var(--accent);text-decoration:none}
pre a:hover{text-decoration:underline}
.line{display:flex;gap:10px;align-items:flex-start;padding:4px 0;border-bottom:1px solid rgba(255,255,255,.04)}
.line:last-child{border-bottom:none}
.line-num{color:var(--dim);font-size:11px;min-width:30px;text-align:right;padding-top:1px;flex-shrink:0}
.line-content{flex:1}
.empty{text-align:center;padding:40px;color:var(--dim);font-size:13px}
.toast{position:fixed;bottom:30px;left:50%;transform:translateX(-50%);background:var(--accent2);color:#fff;padding:10px 24px;border-radius:20px;font-size:13px;z-index:999;opacity:0;transition:opacity .3s;pointer-events:none}
.toast.show{opacity:1}
</style>
</head>
<body>
<div class="ct">
<a class="back" href="/panel">← 返回面板</a>
<h1>整套 ID <span>{{ group }}</span></h1>
<p class="sub">把文件中的 ip:prot 替换为当前代理地址</p>

{% if error %}
<p class="error">{{ error }}</p>
{% else %}
<div class="card">
<div class="actions">
<span class="count">共 {{ lines|length }} 条</span>
<button class="btn btn-primary" onclick="copyAll()">复制全部</button>
</div>
<pre id="content">{% for line in lines %}<div class="line"><span class="line-num">{{ loop.index }}</span><span class="line-content">{{ line }}</span></div>{% endfor %}</pre>
</div>
{% endif %}
</div>
<div class="toast" id="toast">已复制</div>
<script>
function copyAll(){
  var text = '';
  document.querySelectorAll('#content .line-content').forEach(function(el){
    text += el.textContent + '\n';
  });
  if (navigator.clipboard){
    navigator.clipboard.writeText(text.trim()).then(showToast);
  } else {
    var ta = document.createElement('textarea');
    ta.value = text.trim();
    document.body.appendChild(ta);
    ta.select();
    document.execCommand('copy');
    document.body.removeChild(ta);
    showToast();
  }
}
function showToast(){
  var t = document.getElementById('toast');
  t.classList.add('show');
  setTimeout(function(){t.classList.remove('show');}, 2000);
}
</script>
</body>
</html>
'''

PANEL_HTML = r'''
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>IPTV 节点面板</title>
<style>
:root {
  --bg: #0f0f14; --card: #1a1a24; --border: #2a2a3a;
  --text: #c8c8d4; --dim: #6a6a7a; --accent: #6366f1;
  --accent2: #22c55e; --danger: #ef4444; --warn: #eab308;
}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:var(--bg);color:var(--text);min-height:100vh}
.ct{max-width:760px;margin:0 auto;padding:16px 12px 40px}
h1{font-size:18px;text-align:center;margin-bottom:4px;color:#e0e0e8}
h1 span{color:var(--accent)}
.sub{text-align:center;font-size:12px;color:var(--dim);margin-bottom:16px}

/* ── 顶部导航入口 ── */
.nav-links{display:flex;justify-content:center;gap:12px;margin-bottom:8px}
.nav-links a{padding:5px 16px;font-size:12px;border-radius:14px;color:var(--dim);
  text-decoration:none;border:1px solid var(--border);transition:.15s}
.nav-links a:hover{border-color:var(--accent);color:var(--accent)}

/* ── Tab ── */
.tabs{display:flex;gap:0;margin-bottom:10px;background:var(--card);border-radius:10px;padding:4px}
.tab{flex:1;text-align:center;padding:10px 6px;font-size:14px;cursor:pointer;border-radius:7px;transition:.15s;color:var(--dim);border:none;background:transparent;font-weight:500}
.tab.active{background:var(--accent);color:#fff}
.tab .cnt{font-size:11px;opacity:.7;margin-left:3px}

/* ── 子 Tab ── */
.subtabs-wrap{overflow-x:auto;white-space:nowrap;margin-bottom:12px;-webkit-overflow-scrolling:touch;scrollbar-width:none}
.subtabs-wrap::-webkit-scrollbar{display:none}
.subtabs{display:inline-flex;gap:6px;padding:2px 0}
.subtab{padding:6px 14px;font-size:12px;cursor:pointer;border-radius:16px;transition:.15s;color:var(--dim);border:1px solid var(--border);background:transparent;white-space:nowrap;flex-shrink:0}
.subtab.active{background:var(--accent);border-color:var(--accent);color:#fff}
.subtab .cnt2{font-size:10px;opacity:.7;margin-left:2px}
.subtab.has-sel{border-color:var(--accent2);color:var(--accent2)}
.subtab.active.has-sel{background:var(--accent2);border-color:var(--accent2);color:#fff}

/* ── 状态栏 ── */
.bar-wrap{margin-bottom:14px}
.bar{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:12px 14px;margin-bottom:8px}
.bar-top{display:flex;align-items:center;gap:10px;margin-bottom:6px}
.bar-top .dot{width:8px;height:8px;border-radius:50%;flex-shrink:0;background:var(--accent2);box-shadow:0 0 6px var(--accent2)}
.bar-top .gname{font-weight:600;color:var(--accent2);white-space:nowrap;font-size:13px}
.bar-top .gip{color:var(--dim);font-family:monospace;font-size:12px;flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.bar-top .gbtns{display:flex;gap:6px;flex-shrink:0}
.bar-top .gbtn{font-size:11px;padding:4px 12px;border-radius:4px;border:1px solid var(--border);background:transparent;color:var(--dim);cursor:pointer;transition:.15s;white-space:nowrap;display:inline-flex;align-items:center;text-decoration:none}
.bar-top .gbtn:hover{border-color:var(--accent);color:var(--accent)}
.bar-top .gbtn.clear:hover{border-color:var(--danger);color:var(--danger)}
.bar-url{color:var(--accent);font-family:monospace;font-size:11px;cursor:pointer;word-break:break-all;line-height:1.6;padding-left:18px}
.bar-url:hover{text-decoration:underline}
.bar-empty{text-align:center;padding:12px;color:var(--dim);font-size:12px;background:var(--card);border:1px dashed var(--border);border-radius:10px}

/* ── 测速按钮 ── */
.speed-bar{display:flex;align-items:center;gap:8px;margin-bottom:12px}
.speed-bar .speed-all{padding:6px 16px;border-radius:6px;border:1px solid var(--accent);background:transparent;color:var(--accent);font-size:12px;cursor:pointer;transition:.15s}
.speed-bar .speed-all:hover{background:var(--accent);color:#fff}
.speed-bar .speed-all.loading{opacity:.6;pointer-events:none}
.speed-bar .speed-hint{font-size:11px;color:var(--dim)}

/* ── 节点卡片 ── */
.nc{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:12px 16px;margin-bottom:8px;display:flex;align-items:center;gap:12px;cursor:pointer;transition:.15s}
.nc:hover{border-color:var(--accent)}
.nc.sel{border-color:var(--accent2);background:rgba(34,197,94,.06)}
.nc .ni{flex:1;min-width:0}
.nc .ni .name{font-size:14px;font-weight:500}
.nc .ni .ip{font-size:12px;color:var(--dim);font-family:monospace;margin-top:3px}
.nc .ni .extra{font-size:11px;color:var(--dim);margin-top:2px}
.nc .pick{padding:7px 18px;border-radius:6px;border:1px solid var(--accent);background:transparent;color:var(--accent);font-size:12px;cursor:pointer;transition:.15s;flex-shrink:0}
.nc .pick:hover{background:var(--accent);color:#fff}
.nc.sel .pick{background:var(--accent2);border-color:var(--accent2);color:#fff}
.nc .latency{font-size:12px;font-weight:600;padding:4px 10px;border-radius:12px;flex-shrink:0;min-width:52px;text-align:center}
.nc .latency.fast{background:rgba(34,197,94,.15);color:#22c55e}
.nc .latency.mid{background:rgba(34,197,94,.15);color:#eab308}
.nc .latency.slow{background:rgba(239,68,68,.15);color:#ef4444}
.nc .latency.testing{background:rgba(99,102,241,.15);color:var(--accent);animation:pulse .8s infinite}
.nc .latency.none{background:rgba(106,106,122,.1);color:var(--dim)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
.empty{text-align:center;padding:40px;color:var(--dim);font-size:14px}

/* ── Toast ── */
.toast{position:fixed;bottom:30px;left:50%;transform:translateX(-50%);background:var(--accent2);color:#fff;padding:10px 24px;border-radius:20px;font-size:13px;z-index:999;opacity:0;transition:opacity .3s;pointer-events:none}
.toast.show{opacity:1}

/* ── 底部提示 ── */
.tip{text-align:center;font-size:12px;color:var(--dim);margin-top:24px;line-height:1.8}
.tip code{background:var(--card);padding:2px 8px;border-radius:4px;font-size:11px;word-break:break-all}

@media (max-width: 480px) {
  .ct{padding:10px 8px 30px}
  h1{font-size:16px}
  .sub{font-size:11px;margin-bottom:12px}
  .tabs{border-radius:8px;padding:3px}
  .tab{font-size:13px;padding:8px 4px}
  .subtab{font-size:11px;padding:5px 10px}
  .bar{padding:10px 12px}
  .bar-top .gname{font-size:12px}
  .bar-top .gip{font-size:11px}
  .bar-top .gbtn{font-size:10px;padding:3px 10px}
  .bar-url{font-size:10px;padding-left:0}
  .nc{padding:10px 12px;gap:8px;border-radius:8px}
  .nc .ni .name{font-size:13px}
  .nc .ni .ip{font-size:11px}
  .nc .pick{font-size:11px;padding:6px 14px}
  .tip{font-size:11px;margin-top:18px}
  .tip code{font-size:10px;padding:1px 5px}
}
</style>
</head>
<body>
<div class="ct">
<h1>IPTV <span>节点面板</span></h1>
<p class="sub">每个分组独立选择节点，同时生效</p>

<div class="nav-links">
  <a href="/log">📋 日志查看</a>
  <a href="/config">⚙️ 配置编辑</a>
</div>

<div class="tabs">
  <button class="tab active" onclick="switchTab('udp')">UDP 组播 <span class="cnt">({{ udp_nodes|length }})</span></button>
  <button class="tab" onclick="switchTab('hls')">HLS 直播 <span class="cnt">({{ hls_nodes|length }})</span></button>
</div>

<!-- 分组子 TAB -->
<div class="subtabs-wrap" id="subtabs-wrap">
  <div class="subtabs" id="subtabs"></div>
</div>

<!-- 已选状态栏 -->
<div class="bar-wrap" id="bar-wrap"></div>

<!-- 一键测速 -->
<div class="speed-bar" id="speed-bar">
  <button class="speed-all" id="speed-all-btn" onclick="speedTestAll()">⚡ 一键测速</button>
  <span class="speed-hint" id="speed-hint">点击测试当前分组节点延迟</span>
</div>

<!-- 节点列表 -->
<div id="list"></div>

<p class="tip">
  每个分组独立选择节点 → 复制代理地址 → 粘贴到播放器<br>
  <code id="url-hint">http://{{ host }}/udp/分组名/239.77.0.1:5146  |  http://{{ host }}/hls/分组名/hls/2/index.m3u8</code>
</p>
</div>

<div class="toast" id="toast"></div>

<script>
var udpNodes = {{ udp_nodes|tojson|safe }};
var hlsNodes = {{ hls_nodes|tojson|safe }};
var udpGroups = {{ udp_groups|tojson|safe }};
var hlsGroups = {{ hls_groups|tojson|safe }};
var selUdp = {{ sel_udp|tojson|safe }};
var selHls = {{ sel_hls|tojson|safe }};
var selInfoUdp = {{ sel_info_udp|tojson|safe }};
var selInfoHls = {{ sel_info_hls|tojson|safe }};
var host = '{{ host }}';
var ct = 'udp';
var subTab = 'all';

var speedCache = {};
var speedTesting = false;

function $(id){return document.getElementById(id)}

function nodesFor(tt){return tt==='udp'?udpNodes:hlsNodes}
function groupsFor(tt){return tt==='udp'?udpGroups:hlsGroups}
function selFor(tt){return tt==='udp'?selUdp:selHls}
function selInfoFor(tt){return tt==='udp'?selInfoUdp:selInfoHls}

function switchTab(t){
  ct = t;
  document.querySelectorAll('.tab').forEach(function(el,i){
    el.classList.toggle('active', i===(t==='udp'?0:1));
  });
  subTab = 'all';
  buildSubTabs();
  renderAll();
  // 切换到新TAB时自动测速（如果还没测过）
  var ns = getFilteredNodes();
  var needTest = false;
  for (var i=0;i<ns.length;i++){
    if (!speedCache[ns[i].ip_port]){ needTest = true; break; }
  }
  if (needTest){
    setTimeout(function(){ speedTestAll(); }, 300);
  }
}

function buildSubTabs(){
  var groups = groupsFor(ct);
  var allNodes = nodesFor(ct);
  var selObj = selFor(ct);
  var h = '<button class="subtab active" onclick="switchSubTab(\'all\')">全部<span class="cnt2">(' + allNodes.length + ')</span></button>';
  for (var i=0;i<groups.length;i++){
    var g = groups[i];
    var cnt = 0;
    for (var j=0;j<allNodes.length;j++){
      var ng = allNodes[j].name;
      if (ng === g) cnt++;
    }
    var cls = (subTab === g) ? ' active' : '';
    if (selObj[g]) cls += ' has-sel';
    h += '<button class="subtab'+cls+'" onclick="switchSubTab(\''+g.replace(/'/g,"\\'")+'\')">'+g+'<span class="cnt2">('+cnt+')</span></button>';
  }
  $('subtabs').innerHTML = h;
}

function switchSubTab(g){
  subTab = g;
  buildSubTabs();
  renderAll();
}

function getFilteredNodes(){
  var all = nodesFor(ct);
  if (subTab === 'all') return all;
  var filtered = [];
  for (var i=0;i<all.length;i++){
    var ng = all[i].name;
    if (ng === subTab) filtered.push(all[i]);
  }
  return filtered;
}

function pick(ipPort){
  var ns = getFilteredNodes();
  var node = null;
  for (var i=0;i<ns.length;i++){
    if (ns[i].ip_port === ipPort){ node = ns[i]; break; }
  }
  if (!node) return;
  var nodeId = ct==='udp' ? (node.multicast || '') : (node.path || '');
  // UDP和HLS都用name分组
  var group = node.name;
  fetch('/api/select?type='+ct+'&ip_port='+encodeURIComponent(node.ip_port)+'&id='+encodeURIComponent(nodeId)+'&group='+encodeURIComponent(group))
    .then(function(r){return r.json()})
    .then(function(data){
      if (data.ok){
        if (ct === 'udp') { selUdp[group] = node.ip_port; selInfoUdp[group] = {ip_port:node.ip_port, id:data.id, group:group}; }
        else { selHls[group] = node.ip_port; selInfoHls[group] = {ip_port:node.ip_port, id:data.id, group:group}; }
        buildSubTabs();
        renderAll();
        toast('已选择 [' + ct.toUpperCase() + '] ' + group + ' (' + node.ip_port + ')');
      }
    });
}

function clearGroup(group){
  fetch('/api/clear?type='+ct+'&group='+encodeURIComponent(group))
    .then(function(r){return r.json()})
    .then(function(data){
      if (data.ok){
        if (ct === 'udp'){ delete selUdp[group]; delete selInfoUdp[group]; }
        else { delete selHls[group]; delete selInfoHls[group]; }
        buildSubTabs();
        renderAll();
        toast('已清除 [' + ct.toUpperCase() + '] ' + group);
      }
    });
}

function copyUrlForGroup(group){
  var info = selInfoFor(ct)[group];
  var selObj = selFor(ct);
  if (!selObj[group]){ toast('请先选择 ['+group+'] 节点'); return; }
  var nodeId = (info && info.id) ? info.id : (ct==='udp'?'239.77.0.1:5146':'hls/2/index.m3u8');
  var url = 'http://' + host + '/' + ct + '/' + group + '/' + nodeId;
  cp(url);
  toast('['+group+'] 代理地址已复制');
}

function renderAll(){
  renderBars();
  renderList();
  $('url-hint').textContent = ct==='udp' ? 
    'http://' + host + '/udp/分组名/239.77.0.1:5146' :
    'http://' + host + '/hls/分组名/hls/2/index.m3u8';
}

function renderBars(){
  var selObj = selFor(ct);
  var infoObj = selInfoFor(ct);
  var groups = groupsFor(ct);
  var h = '';
  var hasAny = false;
  for (var i=0;i<groups.length;i++){
    var g = groups[i];
    var ipPort = selObj[g];
    if (!ipPort) continue;
    hasAny = true;
    var info = infoObj[g] || {};
    var nodeId = info.id || (ct==='udp'?'239.77.0.1:5146':'hls/2/index.m3u8');
    var url = 'http://' + host + '/' + ct + '/' + g + '/' + nodeId;
    h += '<div class="bar">';
    h += '<div class="bar-top">';
    h += '<div class="dot"></div>';
    h += '<div class="gname">'+g+'</div>';
    h += '<div class="gip">'+ipPort+'</div>';
    h += '<div class="gbtns">';
    h += '<button class="gbtn" onclick="copyUrlForGroup(\''+g.replace(/'/g,"\\'")+'\')">复制</button>';
    h += '<button class="gbtn clear" onclick="clearGroup(\''+g.replace(/'/g,"\\'")+'\')">清除</button>';
    h += '<a class="gbtn" href="/ids/'+encodeURIComponent(g)+'" target="_blank" title="查看整套ID">整套ID</a>';
    h += '</div>';
    h += '</div>';
    h += '<div class="bar-url" onclick="copyUrlForGroup(\''+g.replace(/'/g,"\\'")+'\')" title="点击复制">'+url+'</div>';
    h += '</div>';
  }
  if (!hasAny){
    h = '<div class="bar-empty">尚未选择任何 ' + ct.toUpperCase() + ' 节点，点击下方节点卡片即可选择</div>';
  }
  $('bar-wrap').innerHTML = h;
}

function latencyClass(lat){
  if (lat < 0) return 'none';
  if (lat < 30) return 'fast';
  if (lat < 100) return 'mid';
  return 'slow';
}

function latencyHtml(ipPort){
  var cached = speedCache[ipPort];
  if (!cached) return '<span class="latency none">--</span>';
  if (cached.testing) return '<span class="latency testing">测速中</span>';
  var cls = latencyClass(cached.latency);
  return '<span class="latency '+cls+'">'+cached.text+'</span>';
}

function renderList(){
  var ns = getFilteredNodes();
  var selObj = selFor(ct);
  var h = '';
  if (!ns.length){
    h = '<div class="empty">暂无 ' + (subTab!=='all'?subTab:ct.toUpperCase()) + ' 节点</div>';
  } else {
    for (var i=0;i<ns.length;i++){
      var n = ns[i];
      var ngroup = n.name;
      var isSel = selObj[ngroup] === n.ip_port;
      h += '<div class="nc'+(isSel?' sel':'')+'" onclick="pick(\''+n.ip_port+'\')">';
      h += '<div class="ni"><div class="name">'+n.name+'</div>';
      h += '<div class="ip">'+n.ip_port+'</div>';
      if (n.multicast) h += '<div class="extra">组播: '+n.multicast+'</div>';
      if (n.path) h += '<div class="extra">路径: '+n.path+'</div>';
      h += '</div>';
      h += latencyHtml(n.ip_port);
      h += '<button class="pick">'+(isSel?'已选':'选择')+'</button></div>';
    }
  }
  $('list').innerHTML = h;
}

// ── 并发测速 ──
function speedTestAll(){
  if (speedTesting) return;
  speedTesting = true;
  var btn = $('speed-all-btn');
  var hint = $('speed-hint');
  btn.classList.add('loading');
  btn.textContent = '测速中...';

  var ns = getFilteredNodes();
  if (!ns.length){
    speedTesting = false;
    btn.classList.remove('loading');
    btn.textContent = '⚡ 一键测速';
    toast('暂无节点');
    return;
  }

  // 标记所有为测速中
  var ipPorts = [];
  for (var i=0;i<ns.length;i++){
    var ip = ns[i].ip_port;
    speedCache[ip] = {testing: true};
    ipPorts.push(ip);
  }
  renderList();

  hint.textContent = '0/' + ns.length;

  fetch('/api/speedtest_batch', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ip_ports: ipPorts})
  })
  .then(function(r){return r.json()})
  .then(function(data){
    if (data.ok && data.results){
      for (var i=0;i<data.results.length;i++){
        var r = data.results[i];
        speedCache[r.ip_port] = {latency: r.latency, text: r.text};
      }
    }
    speedTesting = false;
    btn.classList.remove('loading');
    btn.textContent = '⚡ 重新测速';
    hint.textContent = '全部完成，最快 ' + getFastest() + ' | 点击可重新测速';
    renderList();
    toast('测速完成 (' + ns.length + ' 个节点)');
  })
  .catch(function(){
    speedTesting = false;
    btn.classList.remove('loading');
    btn.textContent = '⚡ 一键测速';
    hint.textContent = '测速失败，请重试';
    for (var i=0;i<ns.length;i++){
      speedCache[ns[i].ip_port] = {latency: -1, text: '超时'};
    }
    renderList();
  });
}

function getFastest(){
  var ns = getFilteredNodes();
  var fastest = null;
  for (var i=0;i<ns.length;i++){
    var c = speedCache[ns[i].ip_port];
    if (c && c.latency >= 0){
      if (!fastest || c.latency < fastest.latency) fastest = c;
    }
  }
  return fastest ? fastest.text : '--';
}

function cp(text){
  if (navigator.clipboard){
    navigator.clipboard.writeText(text).catch(function(){fallbackCopy(text)});
  } else {
    fallbackCopy(text);
  }
}
function fallbackCopy(text){
  var ta = document.createElement('textarea');
  ta.value = text; ta.style.position='fixed';ta.style.opacity='0';
  document.body.appendChild(ta); ta.select();
  document.execCommand('copy'); document.body.removeChild(ta);
}

function toast(msg){
  var t = $('toast');
  t.textContent = msg; t.classList.add('show');
  clearTimeout(t._timer);
  t._timer = setTimeout(function(){t.classList.remove('show')}, 2000);
}

buildSubTabs();
renderAll();
// UDP 和 HLS 都自动测速
setTimeout(function(){ speedTestAll(); }, 500);
</script>
</body>
</html>
'''

# ═══════════════════════════════════════════════════════
#  后台扫描器集成
# ═══════════════════════════════════════════════════════

def _run_scanner_loop():
    """在独立线程中运行 asyncio 扫描循环"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # 配置扫描器日志（超过2MB自动轮转，保留最近1个备份）
    scanner_logger = logging.getLogger("scanner")
    scanner_logger.setLevel(logging.INFO)
    if not scanner_logger.handlers:
        fh = RotatingFileHandler(
            SCANNER_LOG_FILE, maxBytes=LOG_MAX_BYTES, backupCount=1,
            encoding='utf-8'
        )
        fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        scanner_logger.addHandler(fh)

    from scanner import main_loop as scanner_main_loop

    scanner_config = CONFIG_FILE
    output_dir = BASE_DIR

    logger = logging.getLogger("scanner")
    logger.info(f"扫描器启动 | config={scanner_config} | output_dir={output_dir}")

    loop.run_until_complete(scanner_main_loop(
        config_file=scanner_config,
        output_dir=output_dir,
        fail_count_file=os.path.join(output_dir, 'fail_count.ini'),
        history_ip_file=os.path.join(output_dir, 'history_ip.txt'),
        scan_interval=SCAN_INTERVAL,
    ))


if __name__ == '__main__':
    print('=' * 50)
    print('  IPTV 流媒体代理 — Flask 集成版')
    print(f'  面板: http://localhost:6603/panel')
    print(f'  日志: http://localhost:6603/log')
    print(f'  UDP:  http://localhost:6603/udp/广东电信/239.77.0.1:5146')
    print(f'  HLS:  http://localhost:6603/hls/河南酒店/hls/2/index.m3u8')
    print(f'  config.ini: {CONFIG_FILE}')
    if ENABLE_SCANNER:
        print(f'  扫描器: 已启用 (间隔 {SCAN_INTERVAL // 60} 分钟)')
    else:
        print(f'  扫描器: 已禁用 (ENABLE_SCANNER=0)')
    print('=' * 50)

    # 启动后台扫描线程
    if ENABLE_SCANNER:
        scanner_thread = threading.Thread(target=_run_scanner_loop, daemon=True, name="scanner-thread")
        scanner_thread.start()
        print('[scanner] 后台扫描线程已启动')

    app.run(host='0.0.0.0', port=6603, debug=True, threaded=True)
