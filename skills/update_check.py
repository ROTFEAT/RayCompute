"""
版本更新检查 — 比较本地版本与 GitHub 远程版本

用法:
    python skills/update_check.py          # 检查更新
    python skills/update_check.py --update # 检查并自动更新
"""
import os
import sys
import subprocess
import urllib.request
import json

SCRIPT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VERSION_FILE = os.path.join(SCRIPT_DIR, "VERSION")


def get_local_version():
    if os.path.exists(VERSION_FILE):
        with open(VERSION_FILE) as f:
            return f.read().strip()
    return "unknown"


def get_remote_version():
    """从 GitHub API 获取远程 VERSION 文件内容"""
    try:
        # 先从 git remote 获取仓库信息
        result = subprocess.run(
            ["git", "-C", SCRIPT_DIR, "remote", "get-url", "origin"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode != 0:
            return None

        remote_url = result.stdout.strip()
        # 解析 owner/repo
        if "github.com" in remote_url:
            # 支持 https 和 ssh 格式
            if remote_url.startswith("https://"):
                parts = remote_url.replace("https://github.com/", "").replace(".git", "").split("/")
            elif "git@github.com:" in remote_url:
                parts = remote_url.replace("git@github.com:", "").replace(".git", "").split("/")
            else:
                return None

            if len(parts) >= 2:
                owner, repo = parts[0], parts[1]
                api_url = f"https://raw.githubusercontent.com/{owner}/{repo}/main/VERSION"
                resp = urllib.request.urlopen(api_url, timeout=5)
                return resp.read().decode().strip()
    except Exception:
        pass
    return None


def version_tuple(v):
    """将版本字符串转为可比较的元组"""
    try:
        return tuple(int(x) for x in v.split("."))
    except (ValueError, AttributeError):
        return (0, 0, 0)


def update():
    """执行 git pull 更新"""
    result = subprocess.run(
        ["git", "-C", SCRIPT_DIR, "pull", "--ff-only"],
        capture_output=True, text=True, timeout=30
    )
    return result.returncode == 0, result.stdout.strip()


def main():
    local = get_local_version()
    remote = get_remote_version()
    do_update = "--update" in sys.argv

    if remote is None:
        # 无法获取远程版本（离线/无权限），静默跳过
        print(f"ray-skills v{local}")
        return

    if version_tuple(remote) > version_tuple(local):
        print(f"ray-skills v{local} → v{remote} 有更新可用")
        if do_update:
            ok, msg = update()
            if ok:
                print(f"  ✓ 已更新到 v{remote}")
                print(f"  {msg}")
            else:
                print(f"  ✗ 自动更新失败，请手动运行: cd {SCRIPT_DIR} && git pull")
        else:
            print(f"  更新命令: cd {SCRIPT_DIR} && git pull")
            print(f"  或运行:   python skills/update_check.py --update")
    else:
        print(f"ray-skills v{local} (最新)")


if __name__ == "__main__":
    main()
