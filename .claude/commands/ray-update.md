# /ray-update - 更新 Ray Skills

手动更新 Ray Skills 到最新版本。

## 执行步骤

```bash
RAY_DIR=$(cat ~/.claude/.ray-skills-path 2>/dev/null || echo "")
```

如果 `RAY_DIR` 为空，提示用户先安装。

### 1. 记录当前版本

```bash
OLD_VER=$(cat $RAY_DIR/VERSION)
echo "当前: v$OLD_VER"
```

### 2. 拉取更新

```bash
cd $RAY_DIR && git pull origin main 2>&1
```

如果 git pull 失败（本地改动），提示：
```bash
cd $RAY_DIR && git stash && git pull origin main && git stash pop
```

### 3. 重新安装（静默执行，不输出 setup 过程）

```bash
cd $RAY_DIR && ./setup 2>&1 | tail -5
```

### 4. 验证更新结果

```bash
NEW_VER=$(cat $RAY_DIR/VERSION)
echo "更新后: v$NEW_VER"
```

### 5. 检查 hooks 状态

```bash
echo "=== Hooks 状态 ==="
if grep -q "ray_compute.py" ~/.claude/settings.json 2>/dev/null; then
    echo "✓ 全局 hooks 已安装"
else
    echo "✗ 全局 hooks 未安装"
fi
if grep -q "Ray 集群" ~/.claude/CLAUDE.md 2>/dev/null; then
    echo "✓ 全局 CLAUDE.md 已包含集群指令"
else
    echo "✗ 全局 CLAUDE.md 缺少集群指令"
fi
```

### 6. 输出总结

格式：
```
已更新：v{old} → v{new}
- hooks: ✓ 已安装
- CLAUDE.md: ✓ 已配置
- skills: /ray-status, /ray-update
```

如果版本没变，说明已是最新。
