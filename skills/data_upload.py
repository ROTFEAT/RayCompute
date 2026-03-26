"""
数据上传工具 — 将本地文件或数据库数据上传到 MinIO，供 Ray 集群任务使用

用法:
    # 上传本地文件
    python skills/data_upload.py upload data.csv --name my-project/data.csv

    # 上传整个目录
    python skills/data_upload.py upload ./data/ --name my-project/data

    # 从数据库导出并上传（MySQL）
    python skills/data_upload.py db "mysql://user:pass@host/db" --query "SELECT * FROM trades" --name my-project/trades.parquet

    # 从数据库导出（PostgreSQL）
    python skills/data_upload.py db "postgresql://user:pass@host/db" --table trades --name my-project/trades.parquet

    # 从 SQLite 导出
    python skills/data_upload.py db "sqlite:///path/to/data.db" --table orders --name my-project/orders.parquet

    # 列出 MinIO 上的数据
    python skills/data_upload.py ls
    python skills/data_upload.py ls my-project/

    # 查看数据信息（行数、列、大小）
    python skills/data_upload.py info my-project/trades.parquet
"""
import argparse
import os
import sys
import json
import io
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from skills.config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE, MINIO_BUCKET


DATA_BUCKET = "ray-data"  # 数据专用 bucket，和结果 bucket 分开


def get_client():
    try:
        from minio import Minio
    except ImportError:
        print("需要安装 minio: pip install minio")
        sys.exit(1)
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                   secret_key=MINIO_SECRET_KEY, secure=MINIO_SECURE)
    if not client.bucket_exists(DATA_BUCKET):
        client.make_bucket(DATA_BUCKET)
    return client


def upload_file(local_path, remote_name):
    """上传单个文件"""
    client = get_client()
    file_size = os.path.getsize(local_path)
    size_mb = round(file_size / 1e6, 1)

    print(f"上传: {local_path} → {DATA_BUCKET}/{remote_name} ({size_mb} MB)")
    start = time.time()
    client.fput_object(DATA_BUCKET, remote_name, local_path)
    elapsed = time.time() - start
    speed = size_mb / elapsed if elapsed > 0 else 0
    print(f"完成: {elapsed:.1f}s ({speed:.1f} MB/s)")

    # 打印 Ray 任务中的使用方式
    print(f"\nRay 任务中读取方式:")
    if remote_name.endswith('.csv'):
        print(f"  import pandas as pd")
        print(f"  from skills.minio_io import read_csv")
        print(f"  df = read_csv('{remote_name}')  # bucket: {DATA_BUCKET}")
    elif remote_name.endswith('.parquet'):
        print(f"  df = read_parquet('{remote_name}')  # 见下方 read_parquet 函数")
    elif remote_name.endswith('.json'):
        print(f"  from skills.minio_io import read_json")
        print(f"  data = read_json('{remote_name}')  # bucket: {DATA_BUCKET}")

    return remote_name


def upload_dir(local_dir, remote_prefix):
    """上传整个目录"""
    uploaded = []
    for root, dirs, files in os.walk(local_dir):
        for f in files:
            local_path = os.path.join(root, f)
            rel = os.path.relpath(local_path, local_dir)
            remote_name = f"{remote_prefix}/{rel}"
            upload_file(local_path, remote_name)
            uploaded.append(remote_name)
    print(f"\n共上传 {len(uploaded)} 个文件")
    return uploaded


def db_export(conn_str, query=None, table=None, remote_name=None, chunk_size=100000):
    """从数据库导出数据到 MinIO"""
    try:
        import pandas as pd
        from sqlalchemy import create_engine, text
    except ImportError:
        print("需要安装: pip install pandas sqlalchemy")
        print("数据库驱动:")
        print("  MySQL:      pip install pymysql")
        print("  PostgreSQL: pip install psycopg2-binary")
        print("  SQLite:     内置，无需安装")
        sys.exit(1)

    if not query and not table:
        print("需要指定 --query 或 --table")
        sys.exit(1)

    if not query:
        query = f"SELECT * FROM {table}"

    if not remote_name:
        name = table or "query_result"
        remote_name = f"data/{name}.parquet"

    print(f"连接数据库: {conn_str.split('@')[-1] if '@' in conn_str else conn_str}")
    engine = create_engine(conn_str)

    # 先查总行数
    with engine.connect() as conn:
        # 尝试获取行数
        try:
            if table:
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            else:
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM ({query}) t"))
            total_rows = count_result.scalar()
            print(f"总行数: {total_rows:,}")
        except Exception:
            total_rows = None
            print("总行数: 未知")

    # 分块读取
    print(f"导出中...")
    chunks = []
    rows_read = 0
    for chunk in pd.read_sql(query, engine, chunksize=chunk_size):
        chunks.append(chunk)
        rows_read += len(chunk)
        if total_rows:
            pct = rows_read / total_rows * 100
            print(f"  已读取: {rows_read:,} / {total_rows:,} ({pct:.0f}%)")
        else:
            print(f"  已读取: {rows_read:,}")

    df = pd.concat(chunks, ignore_index=True)
    print(f"\n数据结构:")
    print(f"  行数: {len(df):,}")
    print(f"  列数: {len(df.columns)}")
    print(f"  列名: {list(df.columns)}")
    print(f"  数据类型:")
    for col in df.columns:
        print(f"    {col}: {df[col].dtype}")

    # 上传到 MinIO
    client = get_client()

    if remote_name.endswith('.parquet'):
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine='pyarrow')
        buf.seek(0)
        size = buf.getbuffer().nbytes
        print(f"\n上传 Parquet: {remote_name} ({round(size/1e6, 1)} MB)")
        client.put_object(DATA_BUCKET, remote_name, buf, size, "application/octet-stream")
    elif remote_name.endswith('.csv'):
        buf = df.to_csv(index=False).encode('utf-8')
        print(f"\n上传 CSV: {remote_name} ({round(len(buf)/1e6, 1)} MB)")
        client.put_object(DATA_BUCKET, remote_name, io.BytesIO(buf), len(buf), "text/csv")
    else:
        # 默认 parquet
        remote_name = remote_name + '.parquet'
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine='pyarrow')
        buf.seek(0)
        size = buf.getbuffer().nbytes
        print(f"\n上传 Parquet: {remote_name} ({round(size/1e6, 1)} MB)")
        client.put_object(DATA_BUCKET, remote_name, buf, size, "application/octet-stream")

    print(f"完成: {DATA_BUCKET}/{remote_name}")

    # 保存 schema 信息
    schema = {
        "rows": len(df),
        "columns": list(df.columns),
        "dtypes": {col: str(df[col].dtype) for col in df.columns},
        "source": conn_str.split('@')[-1] if '@' in conn_str else conn_str,
        "query": query,
    }
    schema_name = remote_name.rsplit('.', 1)[0] + '.schema.json'
    schema_bytes = json.dumps(schema, ensure_ascii=False, indent=2).encode('utf-8')
    client.put_object(DATA_BUCKET, schema_name, io.BytesIO(schema_bytes), len(schema_bytes), "application/json")
    print(f"Schema: {DATA_BUCKET}/{schema_name}")

    # 打印 Ray 任务使用方式
    print(f"\n在 Ray 任务中读取:")
    print(f"  from minio import Minio")
    print(f"  import pandas as pd, io, os")
    print(f"  client = Minio(os.environ['MINIO_ENDPOINT'],")
    print(f"                 access_key=os.environ['MINIO_ACCESS_KEY'],")
    print(f"                 secret_key=os.environ['MINIO_SECRET_KEY'], secure=False)")
    print(f"  resp = client.get_object('{DATA_BUCKET}', '{remote_name}')")
    print(f"  df = pd.read_parquet(io.BytesIO(resp.read()))")
    print(f"  print(f'加载 {{len(df):,}} 行数据')")

    return remote_name, schema


def list_data(prefix=""):
    """列出 MinIO 上的数据文件"""
    client = get_client()
    objects = list(client.list_objects(DATA_BUCKET, prefix=prefix, recursive=True))
    if not objects:
        print(f"bucket '{DATA_BUCKET}' 中无数据" + (f" (前缀: {prefix})" if prefix else ""))
        return

    print(f"{'文件名':<50} {'大小':>10} {'修改时间'}")
    print("-" * 80)
    for obj in objects:
        name = obj.object_name
        size = obj.size
        if size > 1e9:
            size_str = f"{size/1e9:.1f} GB"
        elif size > 1e6:
            size_str = f"{size/1e6:.1f} MB"
        elif size > 1e3:
            size_str = f"{size/1e3:.1f} KB"
        else:
            size_str = f"{size} B"
        mtime = obj.last_modified.strftime("%Y-%m-%d %H:%M") if obj.last_modified else ""
        print(f"{name:<50} {size_str:>10} {mtime}")


def data_info(remote_name):
    """查看数据信息"""
    client = get_client()
    schema_name = remote_name.rsplit('.', 1)[0] + '.schema.json'
    try:
        resp = client.get_object(DATA_BUCKET, schema_name)
        schema = json.loads(resp.read().decode())
        print(f"文件: {DATA_BUCKET}/{remote_name}")
        print(f"行数: {schema['rows']:,}")
        print(f"列数: {len(schema['columns'])}")
        print(f"来源: {schema.get('source', '未知')}")
        print(f"查询: {schema.get('query', '未知')}")
        print(f"\n列详情:")
        for col, dtype in schema['dtypes'].items():
            print(f"  {col}: {dtype}")
    except Exception:
        # 没有 schema 文件，直接看文件大小
        stat = client.stat_object(DATA_BUCKET, remote_name)
        print(f"文件: {DATA_BUCKET}/{remote_name}")
        print(f"大小: {round(stat.size/1e6, 1)} MB")
        print(f"无 schema 信息（非数据库导出的文件）")


def main():
    parser = argparse.ArgumentParser(description="数据上传工具")
    sub = parser.add_subparsers(dest="cmd")

    # upload
    p_upload = sub.add_parser("upload", help="上传本地文件或目录")
    p_upload.add_argument("path", help="本地文件或目录路径")
    p_upload.add_argument("--name", required=True, help="MinIO 上的存储路径")

    # db
    p_db = sub.add_parser("db", help="从数据库导出并上传")
    p_db.add_argument("conn", help="数据库连接字符串 (如 mysql://user:pass@host/db)")
    p_db.add_argument("--query", help="SQL 查询语句")
    p_db.add_argument("--table", help="表名（导出整表）")
    p_db.add_argument("--name", help="MinIO 存储路径 (默认 data/<table>.parquet)")
    p_db.add_argument("--chunk-size", type=int, default=100000, help="分块读取大小")

    # ls
    p_ls = sub.add_parser("ls", help="列出 MinIO 数据")
    p_ls.add_argument("prefix", nargs="?", default="", help="路径前缀")

    # info
    p_info = sub.add_parser("info", help="查看数据信息")
    p_info.add_argument("name", help="MinIO 上的文件路径")

    args = parser.parse_args()

    if args.cmd == "upload":
        if os.path.isdir(args.path):
            upload_dir(args.path, args.name)
        elif os.path.isfile(args.path):
            upload_file(args.path, args.name)
        else:
            print(f"文件不存在: {args.path}")
    elif args.cmd == "db":
        db_export(args.conn, args.query, args.table, args.name, args.chunk_size)
    elif args.cmd == "ls":
        list_data(args.prefix)
    elif args.cmd == "info":
        data_info(args.name)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
