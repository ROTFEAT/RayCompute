"""
Ray 集群压力测试 - 248 CPU 全部跑满
每个 CPU 分配一个计算密集型任务，验证所有节点都在工作
"""
import ray
import time
import numpy as np

@ray.remote(num_cpus=1)
def heavy_task(task_id):
    """CPU 密集型任务：大矩阵 SVD 分解"""
    node_ip = ray.get_runtime_context().get_node_id()
    for node in ray.nodes():
        if node["NodeID"] == node_ip:
            node_ip = node["NodeManagerAddress"]
            break

    start = time.time()
    for i in range(20):
        m = np.random.randn(1000, 1000)
        np.linalg.svd(m)
    elapsed = time.time() - start

    return {
        "task_id": task_id,
        "node_ip": node_ip,
        "duration": round(elapsed, 2),
    }

def main():
    ray.init()

    resources = ray.cluster_resources()
    total_cpu = int(resources.get("CPU", 0))
    nodes = [k for k in resources if k.startswith("node:") and k != "node:__internal_head__"]

    print(f"集群: {total_cpu} CPUs, {len(nodes)} 节点")
    print(f"节点: {', '.join(k.replace('node:', '') for k in sorted(nodes))}")
    print(f"\n提交 {total_cpu} 个任务，每个占 1 CPU...")

    start = time.time()
    futures = [heavy_task.remote(i) for i in range(total_cpu)]

    print(f"全部任务已提交，等待完成...\n")

    results = ray.get(futures)
    total_time = time.time() - start

    # 按节点统计
    node_stats = {}
    for r in results:
        ip = r["node_ip"]
        if ip not in node_stats:
            node_stats[ip] = {"count": 0, "durations": []}
        node_stats[ip]["count"] += 1
        node_stats[ip]["durations"].append(r["duration"])

    print("=" * 60)
    print(f"{'节点 IP':<20} {'任务数':>6} {'平均耗时':>10} {'最大耗时':>10}")
    print("-" * 60)
    for ip in sorted(node_stats):
        s = node_stats[ip]
        avg = np.mean(s["durations"])
        mx = np.max(s["durations"])
        print(f"{ip:<20} {s['count']:>6} {avg:>9.2f}s {mx:>9.2f}s")

    print("=" * 60)
    print(f"总任务数: {len(results)}")
    print(f"参与节点: {len(node_stats)}")
    print(f"总耗时: {total_time:.2f}s")
    print(f"吞吐量: {len(results) / total_time:.1f} tasks/s")

    ray.shutdown()

if __name__ == "__main__":
    main()
