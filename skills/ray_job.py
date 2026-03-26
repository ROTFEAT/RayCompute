"""
Ray 集群任务提交工具

用法:
    python skills/ray_job.py <script.py> [--pip pkg1,pkg2] [--cpus N] [--wait]

示例:
    # 提交任务并等待结果
    python skills/ray_job.py tasks/my_task.py --wait

    # 需要额外依赖
    python skills/ray_job.py tasks/my_task.py --pip optuna,scikit-learn --wait

    # 用自定义镜像（依赖预装，秒启动）
    python skills/ray_job.py tasks/my_task.py --image ml-env --wait

    # 提交后不等待（后台运行）
    python skills/ray_job.py tasks/my_task.py

    # 拉取结果（从 MinIO 下载任务输出）
    python skills/ray_job.py --result <job_id>

    # 管理任务
    python skills/ray_job.py --list
    python skills/ray_job.py --status <job_id>
    python skills/ray_job.py --logs <job_id>
    python skills/ray_job.py --stop <job_id>
"""
import argparse
import subprocess
import os
import json
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from skills.config import RAY_ADDRESS, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET, get

REGISTRY = get("REGISTRY_URL", "")


def submit(script, pip_packages=None, image=None, cpus=0, wait=True):
    abs_script = os.path.abspath(script)
    working_dir = os.path.dirname(abs_script) or "."
    entrypoint = os.path.relpath(abs_script, working_dir)
    cmd = [
        "ray", "job", "submit",
        "--address", RAY_ADDRESS,
        "--working-dir", working_dir,
        "--entrypoint-num-cpus", str(cpus),
    ]

    # 构建 runtime_env，注入 MinIO 配置到集群端环境变量
    runtime_env = {
        "env_vars": {
            "MINIO_ENDPOINT": MINIO_ENDPOINT,
            "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
            "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
            "MINIO_BUCKET": MINIO_BUCKET,
        }
    }
    if pip_packages:
        runtime_env["pip"] = pip_packages
    if image:
        runtime_env["container"] = {
            "image": f"{REGISTRY}/ray-{image}:latest",
            "worker_path": "/home/ray/anaconda3/bin/python",
        }
    cmd += ["--runtime-env-json", json.dumps(runtime_env)]

    # 先用 --no-wait 提交，捕获 Job ID
    submit_cmd = cmd + ["--no-wait", "--", "python", entrypoint]
    env = os.environ.copy()
    env["RAY_ADDRESS"] = RAY_ADDRESS
    result = subprocess.run(submit_cmd, env=env, capture_output=True, text=True)
    print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    if result.returncode != 0:
        return

    # 从输出中提取 Job ID
    job_id = None
    for line in result.stdout.splitlines():
        if line.startswith("Job '") or line.startswith("Job 'raysubmit"):
            job_id = line.split("'")[1]
        elif "raysubmit_" in line:
            for word in line.split():
                if word.startswith("raysubmit_"):
                    job_id = word.strip("'\".,")
                    break

    if job_id:
        # 记录到本地历史文件
        history_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".jobs")
        os.makedirs(history_dir, exist_ok=True)
        history_file = os.path.join(history_dir, "history.jsonl")
        from datetime import datetime
        record = json.dumps({
            "job_id": job_id,
            "script": os.path.basename(script),
            "submitted_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "pip": pip_packages,
            "image": image,
        }, ensure_ascii=False)
        with open(history_file, "a") as f:
            f.write(record + "\n")

        print(f"\n{'='*50}")
        print(f"Job ID: {job_id}")
        print(f"{'='*50}")
        print(f"查看状态: python skills/ray_job.py --status {job_id}")
        print(f"查看日志: python skills/ray_job.py --logs {job_id}")
        print(f"拉取结果: python skills/ray_job.py --result {job_id}")
        print(f"停止任务: python skills/ray_job.py --stop {job_id}")
        print(f"\n已记录到 {history_file}")
        print(f"\n任务已在集群运行，可以安全断开连接或关机。")

    # 如果用户要等待，再 follow 日志
    if wait and job_id:
        print(f"\n等待任务完成...\n")
        subprocess.run(["ray", "job", "logs", "--follow", job_id], env=env)


def job_action(action, job_id):
    env = os.environ.copy()
    env["RAY_ADDRESS"] = RAY_ADDRESS
    subprocess.run(["ray", "job", action, job_id], env=env)


def list_jobs():
    # 先展示本地提交历史
    history_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".jobs", "history.jsonl")
    if os.path.exists(history_file):
        print("=== 本地提交历史 ===")
        with open(history_file) as f:
            lines = f.readlines()
        for line in lines[-20:]:  # 最近 20 条
            try:
                r = json.loads(line.strip())
                pip_info = f"  pip: {','.join(r['pip'])}" if r.get('pip') else ""
                img_info = f"  image: {r['image']}" if r.get('image') else ""
                print(f"  {r['submitted_at']}  {r['job_id']}  {r['script']}{pip_info}{img_info}")
            except (json.JSONDecodeError, KeyError):
                pass
        print()

    # 再查集群状态
    print("=== 集群任务列表 ===")
    env = os.environ.copy()
    env["RAY_ADDRESS"] = RAY_ADDRESS
    subprocess.run(["ray", "job", "list"], env=env)


def fetch_result(job_id):
    """从 MinIO 下载任务结果"""
    try:
        from minio import Minio
    except ImportError:
        print("需要安装 minio: pip install minio")
        return

    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                   secret_key=MINIO_SECRET_KEY, secure=False)

    prefix = f"jobs/{job_id}/"
    objects = list(client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True))

    if not objects:
        print(f"未找到 job {job_id} 的结果。")
        print(f"提示：任务脚本需要调用 save_result() 保存结果，见 skills/template_task.py")
        return

    os.makedirs(f"output/{job_id}", exist_ok=True)
    for obj in objects:
        filename = obj.object_name.replace(prefix, "")
        local_path = f"output/{job_id}/{filename}"
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        client.fget_object(MINIO_BUCKET, obj.object_name, local_path)
        print(f"下载: {obj.object_name} -> {local_path}")

        if filename.endswith(".json"):
            with open(local_path) as f:
                data = json.load(f)
            if isinstance(data, list):
                print(f"  {len(data)} 条记录")
            elif isinstance(data, dict):
                for k, v in list(data.items())[:5]:
                    print(f"  {k}: {v}")


def main():
    parser = argparse.ArgumentParser(description="Ray 集群任务提交工具")
    parser.add_argument("script", nargs="?", help="要提交的 Python 脚本")
    parser.add_argument("--pip", help="额外 pip 依赖，逗号分隔")
    parser.add_argument("--image", help="自定义镜像名称（如 ml-env）")
    parser.add_argument("--cpus", type=int, default=0, help="driver 占用 CPU 数")
    parser.add_argument("--wait", action="store_true", help="等待任务完成")
    parser.add_argument("--result", metavar="JOB_ID", help="拉取任务结果")
    parser.add_argument("--status", metavar="JOB_ID", help="查看任务状态")
    parser.add_argument("--logs", metavar="JOB_ID", help="查看任务日志")
    parser.add_argument("--stop", metavar="JOB_ID", help="停止任务")
    parser.add_argument("--list", action="store_true", help="列出所有任务")

    args = parser.parse_args()

    if args.result:
        fetch_result(args.result)
    elif args.status:
        job_action("status", args.status)
    elif args.logs:
        job_action("logs", args.logs)
    elif args.stop:
        job_action("stop", args.stop)
    elif args.list:
        list_jobs()
    elif args.script:
        pip_packages = args.pip.split(",") if args.pip else None
        submit(args.script, pip_packages, args.image, args.cpus, args.wait)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
