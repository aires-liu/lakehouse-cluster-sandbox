import os
import sys
import subprocess

current_dir = os.path.dirname(os.path.realpath(__file__))
# 获取当前使用的 Python 解释器路径（uv 环境）
python_executable = sys.executable
test_scripts = [f for f in os.listdir(current_dir) \
                if f.startswith("test_") and f.endswith(".py")]
                
for script in test_scripts:
    script_path = os.path.join(current_dir, script)
    print(f"开始运行测试脚本: {script}")
    # subprocess.run返回一个CompletedProcess对象，该对象包括：
    # args：运行的命令
    # returncode：命令的退出码
    # stdout：标准输出
    # stderr：标准错误
    result = subprocess.run([python_executable, script_path])
    # 检查退出码
    if result.returncode != 0:
        print(f"测试脚本 {script} 运行失败！")
        sys.exit(1)
else:
    print("所有测试脚本均运行成功！")
