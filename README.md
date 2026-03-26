# Ray Skills

Claude Code skills for submitting and managing distributed compute tasks on a Ray cluster.

## Install

```bash
git clone https://github.com/ROTFEAT/ray.git && cd ray && ./setup
```

Setup will install dependencies and interactively configure credentials.

## Skills

| Skill | Description |
|-------|-------------|
| `/ray-new` | Generate Ray task scripts (start here if new to Ray) |
| `/ray-push` | Validate and submit tasks to the cluster |
| `/ray-status` | Monitor cluster, manage jobs, fetch results |

## Quick Start

```
# In Claude Code:
/ray-new       # Describe what you want to compute
/ray-push      # Submit to cluster
/ray-status    # Check results
```

## CLI Tools

```bash
python skills/ray_job.py <script.py> --wait         # Submit and wait
python skills/ray_job.py <script.py> --pip pkg1,pkg2 # With dependencies
python skills/ray_job.py --list                       # List jobs
python skills/ray_job.py --result <job_id>            # Fetch results
python skills/ray_job.py --status <job_id>            # Job status
python skills/ray_job.py --logs <job_id>              # Job logs
python skills/ray_job.py --stop <job_id>              # Stop job
```

## Update

```bash
python skills/update_check.py --update
```

Skills auto-check for updates on each run.
