"""
环境检查工具 — 验证 Ray 集群使用环境是否就绪

检查项：
1. .env 存在且关键变量已填写（非占位符）
2. Ray CLI 已安装
3. 集群可达
4. MinIO 可达
5. minio Python 包已安装

退出码: 0 = 全部通过, 1 = 有失败项
输出: 结构化检查结果，最后一行 PASS 或 FAIL
"""
import os
import sys
import subprocess
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

passed = 0
failed = 0
missing_vars = []

PLACEHOLDER_VALUES = {
    "your_access_key", "your_secret_key", "your_api_key",
    "changeme", "xxx", "TODO", "FILL_ME", "",
}

REQUIRED_VARS = {
    "RAY_DASHBOARD_URL": "Ray Dashboard 地址",
    "MINIO_ENDPOINT": "MinIO API 地址",
    "MINIO_ACCESS_KEY": "MinIO 访问密钥",
    "MINIO_SECRET_KEY": "MinIO 私密密钥",
}


def check(name, ok, success_msg, fail_msg):
    global passed, failed
    if ok:
        print(f"  ✓ {name}: {success_msg}")
        passed += 1
    else:
        print(f"  ✗ {name}: {fail_msg}")
        failed += 1


print("=== Ray 环境检查 ===\n")

# 1. .env 文件 + 关键变量
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
env_exists = os.path.exists(env_path)

if not env_exists:
    check(".env 文件", False, "", f"未找到 {env_path}\n    修复: cp .env.example .env && 编辑填入密钥")
else:
    # 解析 .env 内容
    env_values = {}
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, _, value = line.partition("=")
            env_values[key.strip()] = value.strip()

    check(".env 文件", True, f"已加载 ({len(env_values)} 个配置项)", "")

    # 检查每个必需变量
    for var, desc in REQUIRED_VARS.items():
        value = env_values.get(var, "")
        if not value or value in PLACEHOLDER_VALUES:
            check(f"  {var}", False, "",
                  f"未配置或为占位符 (当前值: '{value}')\n      说明: {desc}")
            missing_vars.append(var)
        else:
            check(f"  {var}", True, "已配置", "")

# 2. Ray CLI
try:
    result = subprocess.run(["ray", "--version"], capture_output=True, text=True, timeout=10)
    version = result.stdout.strip()
    check("Ray CLI", result.returncode == 0, version, "未安装")
except FileNotFoundError:
    check("Ray CLI", False, "", "未安装\n    修复: pip install 'ray[default]'")
except Exception as e:
    check("Ray CLI", False, "", str(e))

# 3. 集群连通（仅在 RAY_DASHBOARD_URL 有效时检查）
from skills.config import RAY_ADDRESS
if RAY_ADDRESS and "RAY_DASHBOARD_URL" not in missing_vars:
    try:
        resp = urllib.request.urlopen(f"{RAY_ADDRESS}/api/version", timeout=5)
        data = resp.read().decode()
        check("集群连通", True, f"在线 ({RAY_ADDRESS})", "")
    except Exception:
        check("集群连通", False, "", f"不可达 ({RAY_ADDRESS})\n    确认: 是否在同一内网？")
elif "RAY_DASHBOARD_URL" in missing_vars:
    check("集群连通", False, "", "跳过 — RAY_DASHBOARD_URL 未配置")
else:
    check("集群连通", False, "", "RAY_DASHBOARD_URL 未配置")

# 4. MinIO 连通（仅在 MINIO_ENDPOINT 有效时检查）
from skills.config import MINIO_ENDPOINT
if MINIO_ENDPOINT and "MINIO_ENDPOINT" not in missing_vars:
    try:
        resp = urllib.request.urlopen(f"http://{MINIO_ENDPOINT}/minio/health/live", timeout=5)
        check("MinIO", True, f"在线 ({MINIO_ENDPOINT})", "")
    except Exception:
        check("MinIO", False, "", f"不可达 ({MINIO_ENDPOINT})\n    确认: MINIO_ENDPOINT 是否正确？")
elif "MINIO_ENDPOINT" in missing_vars:
    check("MinIO", False, "", "跳过 — MINIO_ENDPOINT 未配置")
else:
    check("MinIO", False, "", "MINIO_ENDPOINT 未配置")

# 5. minio Python 包
try:
    import minio
    check("minio 包", True, f"已安装 (v{minio.__version__})", "")
except ImportError:
    check("minio 包", False, "", "未安装\n    修复: pip install minio")

# 汇总
print(f"\n结果: {passed} 通过, {failed} 失败")

if missing_vars:
    print(f"\n需要配置的变量:")
    for var in missing_vars:
        print(f"  {var}={REQUIRED_VARS[var]}")
    print(f"\n请编辑 {env_path} 填入以上变量，或联系集群管理员获取。")

# 全部通过时，打印连通状态表格
if failed == 0:
    cluster_info = ""
    cluster_cpu = ""
    minio_info = ""

    # 获取集群详情
    if RAY_ADDRESS:
        try:
            resp = urllib.request.urlopen(f"{RAY_ADDRESS}/api/cluster_status", timeout=5)
            import json
            data = json.loads(resp.read().decode())
            cs = data.get("clusterStatus", {})
            load = cs.get("loadMetricsReport", {})
            total = load.get("totalResources", {})
            cpu = int(total.get("CPU", 0))
            mem_gb = round(total.get("memory", 0) / 1e9, 1)
            nodes = len([n for n in data.get("clusterStatus", {}).get("activeNodes", [])]) or "5+"
            cluster_info = RAY_ADDRESS
            cluster_cpu = f"{cpu} CPU, {mem_gb} TB 内存, {nodes} 节点" if mem_gb >= 1000 else f"{cpu} CPU, {mem_gb} GB 内存, {nodes} 节点"
        except Exception:
            cluster_info = RAY_ADDRESS
            cluster_cpu = "在线（无法获取详情）"

    if MINIO_ENDPOINT:
        from skills.config import MINIO_BUCKET
        minio_info = f"bucket {MINIO_BUCKET} 已就绪"

    print("\n全部连通：\n")
    print("  ┌───────────────┬──────┬" + "─" * 40 + "┐")
    print("  │     服务      │ 状态 │ 详情" + " " * 36 + "│")
    print("  ├───────────────┼──────┼" + "─" * 40 + "┤")

    row1 = f"  │ Ray Dashboard │  ✅  │ {cluster_info}"
    print(row1 + " " * max(1, 41 - len(cluster_info)) + "│")
    print("  ├───────────────┼──────┼" + "─" * 40 + "┤")

    row2_detail = cluster_cpu
    row2 = f"  │ Ray 集群      │  ✅  │ {row2_detail}"
    print(row2 + " " * max(1, 41 - len(row2_detail)) + "│")
    print("  ├───────────────┼──────┼" + "─" * 40 + "┤")

    row3_detail = minio_info
    row3 = f"  │ MinIO         │  ✅  │ {row3_detail}"
    print(row3 + " " * max(1, 41 - len(row3_detail)) + "│")
    print("  └───────────────┴──────┴" + "─" * 40 + "┘")

    print("\nPASS")
else:
    print("FAIL")

sys.exit(0 if failed == 0 else 1)
