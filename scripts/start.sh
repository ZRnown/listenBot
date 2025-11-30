#!/bin/bash

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# 切换到项目根目录
cd "$PROJECT_ROOT"

# 检测 Python 解释器
if [ -f "venv/bin/python3" ]; then
    PYTHON="venv/bin/python3"
elif [ -f "venv/bin/python" ]; then
    PYTHON="venv/bin/python"
elif command -v python3 &> /dev/null; then
    PYTHON="python3"
elif command -v python &> /dev/null; then
    PYTHON="python"
else
    echo "错误: 未找到 Python 解释器"
    exit 1
fi

echo "使用 Python: $PYTHON"
echo "项目目录: $PROJECT_ROOT"

# 运行启动脚本
exec "$PYTHON" "$PROJECT_ROOT/scripts/start.py"

