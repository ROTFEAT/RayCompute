# Ray Skills

通过 Claude Code 向共享 Ray 集群提交分布式计算任务。无需学习 Ray。

## 安装

```bash
git clone https://github.com/ROTFEAT/ray.git && cd ray && ./setup
```

## 快速开始

**第一步** — 生成任务脚本：
```
/ray-new
```
描述你想做的计算，Claude 生成完整的 Ray 脚本。

**第二步** — 提交到集群：
```
/ray-push
```
自动识别刚生成的脚本，验证、提交。提交成功后可以关机——任务在集群运行，不依赖你的电脑。

**第三步** — 回来看结果：
```
/ray-status
```
查看任务状态、下载结果。Job ID 已保存在本地，随时可查。

## Skills

| Skill | 功能 |
|-------|------|
| `/ray-new` | 从自然语言生成 Ray 脚本，或将现有 Python 代码并行化 |
| `/ray-push` | 验证 + 提交任务（22 项检查清单，自动依赖检测） |
| `/ray-status` | 集群监控、任务管理、结果拉取、失败分析 |

## CLI

```bash
python skills/ray_job.py <script.py> --wait          # 提交并等待
python skills/ray_job.py <script.py> --pip pkg1,pkg2  # 带依赖提交
python skills/ray_job.py --list                        # 列出任务（含本地历史）
python skills/ray_job.py --result <job_id>             # 下载结果
python skills/ray_job.py --logs <job_id>               # 查看日志
python skills/ray_job.py --stop <job_id>               # 停止任务
```

## 工作机制

每个 `/ray-*` skill 执行前自动运行：

1. **版本检查** — 有更新时通知
2. **环境检查** — 验证 `.env` 配置、集群连通性、依赖完整性

`.env` 缺失或包含占位符时会引导运行 `./setup` 交互式配置密钥。

Job ID 持久化到 `.jobs/history.jsonl`，关闭终端或重启后仍可找回。

## 更新

```bash
python skills/update_check.py --update
```
