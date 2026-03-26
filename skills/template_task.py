"""
Ray 任务模板 - 复制此文件到 tasks/ 目录作为新任务的起点

提交: python skills/ray_job.py tasks/my_task.py --pip minio --wait
拿结果: python skills/ray_job.py --result <job_id>
"""
import ray
import numpy as np
import json
import io
import os


def save_result(data, filename="result.json"):
    """
    保存结果到 MinIO，按 job_id 自动归档。
    拿结果: python skills/ray_job.py --result <job_id>

    需要通过环境变量传入 MinIO 配置（提交任务时用 --pip minio）:
      MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET
    """
    from minio import Minio

    endpoint = os.environ.get("MINIO_ENDPOINT", "")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "")
    bucket = os.environ.get("MINIO_BUCKET", "ray-result")
    # RAY_JOB_ID 由 Ray Jobs API 自动注入
    job_id = os.environ.get("RAY_JOB_ID", ray.get_runtime_context().get_job_id())

    object_name = f"jobs/{job_id}/{filename}"
    client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    if filename.endswith(".csv"):
        buf = data.to_csv(index=False).encode("utf-8")
        content_type = "text/csv"
    else:
        buf = json.dumps(data, ensure_ascii=False, indent=2, default=str).encode("utf-8")
        content_type = "application/json"

    client.put_object(bucket, object_name, io.BytesIO(buf), len(buf), content_type)
    print(f"结果已保存: {bucket}/{object_name}")
    print(f"拉取命令: python skills/ray_job.py --result {job_id}")


# ============ 你的任务逻辑 ============

@ray.remote(num_cpus=1)
def compute(task_id, params):
    """单个计算任务 - 在集群某个节点上执行"""
    result = params["x"] ** 2 + params["y"] ** 2
    return {"task_id": task_id, "params": params, "result": result}


def main():
    ray.init()

    param_list = [
        {"x": np.random.uniform(-10, 10), "y": np.random.uniform(-10, 10)}
        for _ in range(100)
    ]

    futures = [compute.remote(i, p) for i, p in enumerate(param_list)]
    results = ray.get(futures)

    best = min(results, key=lambda r: r["result"])
    print(f"Best: task_id={best['task_id']}, result={best['result']:.4f}")

    # 保存结果到 MinIO
    save_result(results, "result.json")
    save_result({"best": best, "total_tasks": len(results)}, "summary.json")

    ray.shutdown()


if __name__ == "__main__":
    main()
