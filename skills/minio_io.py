"""
MinIO 读写工具

用法:
    from skills.minio_io import upload, download, list_files
    from skills.minio_io import upload_json, read_json
    from skills.minio_io import upload_df, read_csv
"""
import io
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from skills.config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE, MINIO_BUCKET


def get_client():
    try:
        from minio import Minio
    except ImportError:
        print("需要安装 minio: pip install minio")
        raise SystemExit(1)
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                 secret_key=MINIO_SECRET_KEY, secure=MINIO_SECURE)


def _ensure_bucket(client):
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)


def upload(object_name, file_path):
    client = get_client()
    _ensure_bucket(client)
    client.fput_object(MINIO_BUCKET, object_name, file_path)
    print(f"Uploaded: {object_name}")


def download(object_name, file_path):
    client = get_client()
    client.fget_object(MINIO_BUCKET, object_name, file_path)
    print(f"Downloaded: {object_name} -> {file_path}")


def upload_bytes(object_name, data: bytes, content_type="application/octet-stream"):
    client = get_client()
    _ensure_bucket(client)
    client.put_object(MINIO_BUCKET, object_name, io.BytesIO(data), len(data), content_type)
    print(f"Uploaded: {object_name}")


def upload_json(object_name, obj):
    data = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
    upload_bytes(object_name, data, "application/json")


def read_json(object_name):
    client = get_client()
    resp = client.get_object(MINIO_BUCKET, object_name)
    return json.loads(resp.read().decode("utf-8"))


def upload_df(object_name, df):
    data = df.to_csv(index=False).encode("utf-8")
    upload_bytes(object_name, data, "text/csv")


def read_csv(object_name):
    import pandas as pd
    client = get_client()
    resp = client.get_object(MINIO_BUCKET, object_name)
    return pd.read_csv(io.BytesIO(resp.read()))


def list_files(prefix=""):
    client = get_client()
    objects = client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True)
    return [obj.object_name for obj in objects]


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "ls":
        prefix = sys.argv[2] if len(sys.argv) > 2 else ""
        for f in list_files(prefix):
            print(f)
    else:
        print("用法: python skills/minio_io.py ls [prefix]")
