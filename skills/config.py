"""
统一配置 - 从 ray 项目自己的 .env 文件加载
不会污染 os.environ，不会覆盖其他项目的环境变量
"""
import os
from pathlib import Path

_config = {}


def _load_env():
    """加载 ray 项目根目录的 .env 到内部字典（不写入 os.environ）"""
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, _, value = line.partition("=")
            key, value = key.strip(), value.strip()
            if key:
                _config[key] = value


def get(key, default=""):
    """优先读 os.environ（用户显式设置），其次读 .env 文件"""
    return os.environ.get(key, _config.get(key, default))


_load_env()

# Ray
RAY_ADDRESS = get("RAY_DASHBOARD_URL")

# MinIO
MINIO_ENDPOINT = get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = get("MINIO_SECRET_KEY")
MINIO_SECURE = get("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = get("MINIO_BUCKET", "ray-result")
