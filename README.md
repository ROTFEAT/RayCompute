# Ray Skills

Claude Code skills for a shared Ray cluster. Submit distributed compute tasks without learning Ray.

## Install

```bash
git clone https://github.com/ROTFEAT/ray.git && cd ray && ./setup
```

## Quick Start

**Step 1** — Generate a task script:
```
/ray-new
```
Describe what you want to compute. Claude generates a complete Ray script.

**Step 2** — Submit to cluster:
```
/ray-push tasks/your_task.py
```
Validates the script, identifies dependencies, submits. Prints Job ID.

**Step 3** — Fetch results:
```
/ray-status
```
Check cluster health, view logs, download results. Works after disconnect — tasks run on the cluster, not your machine.

## Skills

| Skill | What it does |
|-------|-------------|
| `/ray-new` | Generate Ray scripts from natural language, or parallelize existing Python code |
| `/ray-push` | Validate + submit tasks (22-point checklist, auto-dependency detection) |
| `/ray-status` | Cluster monitoring, job management, result retrieval, failure analysis |

## CLI

```bash
python skills/ray_job.py <script.py> --wait          # Submit and wait
python skills/ray_job.py <script.py> --pip pkg1,pkg2  # With dependencies
python skills/ray_job.py --list                        # List jobs (includes local history)
python skills/ray_job.py --result <job_id>             # Download results
python skills/ray_job.py --logs <job_id>               # View logs
python skills/ray_job.py --stop <job_id>               # Stop a job
```

## How It Works

Every `/ray-*` skill runs two checks before doing anything:

1. **Version check** — notifies if an update is available
2. **Environment check** — verifies `.env` config, cluster connectivity, dependencies

If `.env` is missing or contains placeholder values, you'll be prompted to run `./setup` which interactively configures credentials.

Job IDs are persisted to `.jobs/history.jsonl` so you can find them after closing your terminal or restarting your machine.

## Update

```bash
python skills/update_check.py --update
```
