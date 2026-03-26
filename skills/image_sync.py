"""
镜像同步工具 — 将 Docker 镜像分发到所有 Worker 节点

策略：先推到内网 Registry，再让各节点从内网拉（比 Docker Hub 快 10x+）

用法:
    # 同步 Ray 基础镜像到所有节点
    python skills/image_sync.py rayproject/ray:2.54.0-py312

    # 同步自定义镜像
    python skills/image_sync.py my-env

    # 查看各节点镜像状态
    python skills/image_sync.py --status

    # 指定节点
    python skills/image_sync.py rayproject/ray:2.54.0-py312 --nodes 192.168.3.165,192.168.3.223
"""
import argparse
import subprocess
import sys
import os
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from skills.config import get

REGISTRY = get("REGISTRY_URL", "")
SSH_USER = "hywl"
SSH_PASS = "123"

# 从集群 API 自动获取 Worker 节点 IP
def get_worker_ips():
    dashboard = get("RAY_DASHBOARD_URL", "")
    if not dashboard:
        return []
    try:
        import urllib.request
        resp = urllib.request.urlopen(f"{dashboard}/api/v0/nodes", timeout=5)
        data = json.loads(resp.read().decode())
        nodes = data["data"]["result"]["result"]
        return [n["node_ip"] for n in nodes if not n["is_head_node"]]
    except Exception:
        return []


def ssh_cmd(ip, cmd):
    """通过 SSH 在远程节点执行命令"""
    return subprocess.run(
        ["sshpass", "-p", SSH_PASS, "ssh",
         "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=10",
         f"{SSH_USER}@{ip}", f"sg docker -c '{cmd}'"],
        capture_output=True, text=True, timeout=300
    )


def check_image_on_node(ip, image):
    """检查节点上是否已有某镜像"""
    result = ssh_cmd(ip, f"docker images -q {image}")
    return bool(result.stdout.strip())


def show_status(image, nodes):
    """显示各节点的镜像状态"""
    print(f"\n镜像: {image}")
    print(f"{'节点 IP':<18} {'状态':<8} {'大小'}")
    print("-" * 45)
    for ip in nodes:
        result = ssh_cmd(ip, f"docker images {image} --format '{{{{.Size}}}}'")
        size = result.stdout.strip()
        if size:
            print(f"{ip:<18} {'✓ 已有':<8} {size}")
        else:
            print(f"{ip:<18} {'✗ 缺失':<8}")


def sync_image(image, nodes):
    """同步镜像到所有节点"""
    if not REGISTRY:
        print("警告: REGISTRY_URL 未配置，将直接从 Docker Hub 拉取（较慢）")
        direct_pull(image, nodes)
        return

    registry_tag = f"{REGISTRY}/{image.replace('/', '_').replace(':', '_')}"

    # Step 1: 本地 tag + push 到内网 Registry
    print(f"\n[1/3] 推送到内网 Registry ({REGISTRY})...")
    head_ip = get("RAY_HEAD_IP", "")
    if head_ip:
        # 在 Head 节点操作（它一般已经有镜像了）
        result = ssh_cmd(head_ip, f"docker tag {image} {registry_tag} && docker push {registry_tag}")
        if result.returncode != 0:
            # Head 没有镜像，先拉再推
            print(f"  Head 节点没有 {image}，先拉取...")
            ssh_cmd(head_ip, f"docker pull {image}")
            result = ssh_cmd(head_ip, f"docker tag {image} {registry_tag} && docker push {registry_tag}")
            if result.returncode != 0:
                print(f"  推送失败: {result.stderr.strip()}")
                print("  降级为直接拉取模式")
                direct_pull(image, nodes)
                return
        print(f"  ✓ 已推送到 {registry_tag}")
    else:
        print("  RAY_HEAD_IP 未配置，降级为直接拉取")
        direct_pull(image, nodes)
        return

    # Step 2: 各节点从内网 Registry 拉取
    print(f"\n[2/3] 各节点从内网拉取...")
    import concurrent.futures

    def pull_on_node(ip):
        if check_image_on_node(ip, image):
            return ip, "已有", ""
        result = ssh_cmd(ip, f"docker pull {registry_tag} && docker tag {registry_tag} {image}")
        if result.returncode == 0:
            return ip, "成功", ""
        else:
            return ip, "失败", result.stderr.strip()[:100]

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(pull_on_node, ip): ip for ip in nodes}
        for future in concurrent.futures.as_completed(futures):
            ip, status, err = future.result()
            if status == "已有":
                print(f"  {ip}: ✓ 已有，跳过")
            elif status == "成功":
                print(f"  {ip}: ✓ 从内网拉取成功")
            else:
                print(f"  {ip}: ✗ 失败 — {err}")

    # Step 3: 验证
    print(f"\n[3/3] 验证...")
    show_status(image, nodes)


def direct_pull(image, nodes):
    """直接从 Docker Hub 拉取（无 Registry fallback）"""
    import concurrent.futures

    def pull_on_node(ip):
        if check_image_on_node(ip, image):
            return ip, "已有"
        result = ssh_cmd(ip, f"docker pull {image}")
        return ip, "成功" if result.returncode == 0 else "失败"

    print(f"直接从 Docker Hub 拉取到各节点（可能较慢）...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(pull_on_node, ip): ip for ip in nodes}
        for future in concurrent.futures.as_completed(futures):
            ip, status = future.result()
            print(f"  {ip}: {'✓' if status != '失败' else '✗'} {status}")


def main():
    parser = argparse.ArgumentParser(description="镜像同步工具")
    parser.add_argument("image", nargs="?", help="要同步的镜像（如 rayproject/ray:2.54.0-py312）")
    parser.add_argument("--status", action="store_true", help="查看各节点镜像状态")
    parser.add_argument("--nodes", help="指定节点 IP，逗号分隔（默认自动获取所有 Worker）")

    args = parser.parse_args()

    # 获取节点列表
    if args.nodes:
        nodes = args.nodes.split(",")
    else:
        nodes = get_worker_ips()
        if not nodes:
            print("无法获取 Worker 节点列表，请用 --nodes 手动指定")
            sys.exit(1)

    print(f"Worker 节点: {', '.join(nodes)}")

    if args.status:
        if not args.image:
            args.image = "rayproject/ray:2.54.0-py312"
        show_status(args.image, nodes)
    elif args.image:
        sync_image(args.image, nodes)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
