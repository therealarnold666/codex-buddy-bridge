# codex-buddy-bridge

把 [Codex](https://developers.openai.com/codex) 的 hooks、session 状态和 BLE buddy 设备桥接起来，让 StickS3 之类的小设备可以显示审批、运行态和等待态。

> **English**: [README.en.md](README.en.md)

## 关联仓库

- 固件仓库：[`therealarnold666/claude-desktop-buddy-s3`](https://github.com/therealarnold666/claude-desktop-buddy-s3)
- 当前仓库负责 host 侧 daemon、hooks、状态同步与 BLE 桥接。
- 固件仓库负责 StickS3 端 UI、动画、蜂鸣器、功耗策略和 BLE 外设实现。

## 相对上游 fork 的改动

相比上游 `Yamiqu/codex-buddy-bridge`，本仓库增加了：

- Windows Codex Desktop 兼容：Windows 下使用本地 TCP IPC 端点 `tcp://127.0.0.1:8876`，并通过 PowerShell hook wrapper 统一注入 `CODEX_BUDDY_SOCKET`。
- hook 覆盖从 `PermissionRequest` 扩展到 `SessionStart + UserPromptSubmit + Stop`，并在 daemon 内维护 turn 级运行态。
- `running` 改为按 turn 统计，同一 session 持续对话时也能正确反映 busy / idle。
- `sessions`、token、interactive waiting 可从 `~/.codex/sessions/**/*.jsonl` 增量扫描补齐。
- token 账本持久化：保存 lifetime total、`tokens_today`、以及每个 session 的绝对 output total。
- BLE 状态同步增加重试窗口，减少 duty-cycle 广播场景下的 busy->idle 漏同步。
- Linux 新增 `codex-buddy pair` 辅助命令，用 `bluetoothctl` 做 pair / trust / connect。

## Interactive Waiting 说明

- 当前稳定可依赖的 hooks 仍然是：
  `PermissionRequest / SessionStart / UserPromptSubmit / Stop`
- `InteractiveStart` 和 `InteractiveEnd` 依然会被安装脚本注册，但它们目前主要是前向兼容占位。
  也就是说，未来如果某些 Codex 构建开始稳定发这两个事件，bridge 不用再改安装配置。
- 目前稳定路径里，interactive waiting 仍主要通过扫描
  `~/.codex/sessions/**/*.jsonl` 中的 `request_user_input` 及其结束事件来推断。
- 当前 stick 对 interactive 的支持是 **提示型** 而不是 **提交型**：
  它可以显示 `input needed` / `choice needed`，但答案仍需要在 Codex Desktop UI 里提交。

## 工作方式

Codex 会在审批前触发 `PermissionRequest` hook，本项目把这类事件转发到 BLE buddy：

```text
Codex (CLI / Desktop)
  -> hook / session event
  -> daemon
  -> BLE NUS
  -> buddy firmware
```

- POSIX 默认 IPC：`/tmp/codex-buddy.sock`
- Windows 默认 IPC：`tcp://127.0.0.1:8876`

daemon 平时不会长时间占用 BLE；只有真正需要审批或状态同步时才短暂连接设备，之后马上释放。

## 环境要求

- macOS 或 Windows
- Python 3.9+
- 一台名称前缀为 `Claude-` 的 BLE 设备，运行
  [anthropics/claude-desktop-buddy](https://github.com/anthropics/claude-desktop-buddy)
  协议兼容固件
- Codex CLI 或 Codex Desktop，建议使用 2026 年 4 月之后带 stable hooks 的版本

## 安装

### macOS

```bash
git clone https://github.com/Yamiqu/codex-buddy-bridge.git
cd codex-buddy-bridge
./install.sh
```

安装脚本会：

1. 创建 `.venv/` 并安装依赖
2. 渲染并加载 `launchd` plist
3. 在 `~/.codex/config.toml` 中启用 `codex_hooks = true`
4. 写入 `~/.codex/hooks.json`
5. 注册 `PermissionRequest / SessionStart / UserPromptSubmit / Stop / InteractiveStart / InteractiveEnd`

安装后：

- 重启 Codex Desktop 和已打开的 Codex CLI session
- 首次触发 BLE 访问时，允许 macOS 对 Python 解释器的蓝牙权限请求

### Windows

```powershell
git clone https://github.com/Yamiqu/codex-buddy-bridge.git
cd codex-buddy-bridge
powershell -ExecutionPolicy Bypass -File .\install_windows.ps1
```

Windows 安装脚本会：

1. 创建 `.venv\` 并安装依赖
2. 在 `~/.codex/config.toml` 中启用 `codex_hooks = true`
3. 写入 `~/.codex/hooks.json`
4. 通过 `scripts/hook_wrapper.ps1` 调用每个 Python hook
5. 统一把 hook 目标 IPC 端点设置为 `tcp://127.0.0.1:8876`

安装后：

- 重启 Codex Desktop，让 app-server 重新加载 hooks 配置
- 启动 daemon：

```powershell
.venv\Scripts\python.exe -m codex_buddy_bridge --socket tcp://127.0.0.1:8876 --debug
```

## CLI

常用命令：

- `codex-buddy status`
- `codex-buddy on`
- `codex-buddy off`
- `codex-buddy restart`
- `codex-buddy log`
- `codex-buddy foreground`
- `codex-buddy probe`
- `codex-buddy pair`
- `codex-buddy uninstall`

## 验证

```bash
PYTHONPATH=. python3 -m unittest discover -s tests
```

Windows 下也可以单独跑：

```powershell
python -m unittest tests.test_ipc tests.test_daemon
```

## Daemon 参数

- `--device-prefix`
- `--address`
- `--socket`
  POSIX 默认 `/tmp/codex-buddy.sock`
  Windows 默认 `tcp://127.0.0.1:8876`
- `--debug`

## 兼容性说明

- Codex Desktop 和 CLI 都可以使用这套 bridge。
- Windows 下不使用 Unix socket，而是本地 TCP 端点。
- interactive prompt 当前只在 stick 上做状态提示，不支持在 stick 上直接完成回答提交。
- 固件协议保持兼容，不要求修改设备端 wire format。

## License

[MIT](LICENSE)
