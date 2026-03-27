# /ray-update - 更新 Ray Skills

手动更新 Ray Skills 到最新版本。

## 执行步骤

```bash
RAY_DIR=$(cat ~/.claude/.ray-skills-path 2>/dev/null || echo "")
```

如果 `RAY_DIR` 为空，提示用户先安装。

### 1. 检查当前版本

```bash
cat $RAY_DIR/VERSION
```

### 2. 拉取更新

```bash
cd $RAY_DIR && git pull origin main 2>&1
```

如果 git pull 失败（可能有本地改动），提示：
```bash
cd $RAY_DIR && git stash && git pull origin main && git stash pop
```

### 3. 重新安装

```bash
cd $RAY_DIR && ./setup
```

这会更新全局 skill 文件和 CLAUDE.md 指令。

### 4. 显示更新结果

```bash
cat $RAY_DIR/VERSION
```

告诉用户：
- 更新前版本
- 更新后版本
- 如果版本没变，说明已是最新
