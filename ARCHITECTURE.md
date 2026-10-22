# Pi 架构地图

pi 是一个 TypeScript monorepo（npm workspaces），核心产物是命令行 coding agent `pi`，其余包围绕它提供模型接入、终端 UI、沙箱执行、持久化运行时和远程环境能力。

## 依赖分层

从底层到上层，依赖方向基本单向：

```
telemetry (契约)          chord (多进程对象/热重载/复制状态)
      │                          │
      └──────────┬───────────────┘
                 │
   ai ── agent ──┤
                 ├── codemode (QuickJS 沙箱)
                 ├── mcp      (MCP 客户端)
                 ├── tui      (终端 UI 框架)
                 ├── durable  (持久化 agent 运行时，基于 chord)
                 └── env      (durable 的远程执行环境，含 Rust daemon)

coding-agent (CLI，依赖 ai / agent / codemode / mcp / tui)
evals (行为评测，驱动 coding-agent 与 docs)
```

## 根目录

| 路径 | 用途 |
| --- | --- |
| `packages/` | 所有可发布的 npm 包（见下节） |
| `scripts/` | 仓库级构建、检查、发布脚本（见下节） |
| `.github/` | CI 工作流、issue 模板、贡献者白名单 |
| `.github/workflows/` | CI、二进制构建、nix、npm audit、模型目录发布、issue/PR 自动门禁 |
| `.github/ISSUE_TEMPLATE/` | bug、配置、贡献、包问题报告的模板 |
| `.husky/` | `pre-commit` 钩子：锁文件检查 + `npm run check` + 浏览器 smoke |
| `.pi/` | pi 自身的项目级配置（它自己也是 pi 工作区） |
| `nix/` | Nix 打包描述（`package.nix`、模型目录固定版本） |

### 根文件

- `package.json`：workspace 定义、`build` / `check` / `test` / `release` 入口。构建顺序固定为 telemetry → chord → tui → codemode → mcp → ai → durable → env → agent → coding-agent。
- `tsconfig.json`、`tsconfig.base.json`：共享 TypeScript 配置（启用 Node strip-only，只用可擦除语法）。
- `biome.json`：格式化与 lint 配置。
- `vitest.base.ts`、`test.sh`：非 e2e 测试的统一入口。
- `flake.nix` / `flake.lock`：Nix flake 入口。
- `pi-test.{sh,ps1,bat}`：用本地源码启动 pi 做手工测试。
- `CONTRIBUTING.md`、`SECURITY.md`、`AGENTS.md`：贡献门禁、安全策略、开发规则。

## .pi/（本仓库自己的 pi 资源）

| 路径 | 用途 |
| --- | --- |
| `.pi/extensions/` | 开发期自用扩展（TPS 显示、URL 提示、重绘调试、导入复现） |
| `.pi/skills/` | 仓库内技能：`add-llm-provider.md`、`github-work.md`、`interactive-testing.md`、`release.md` |
| `.pi/prompts/` | 提示模板（`pr`、`wr`、`cl`、`sa`、`is`、`dup`、`deslop`） |
| `.pi/git/`、`.pi/npm/` | 占位目录（仅保留 `.gitignore`），用于隔离工具缓存 |

## packages/

### packages/coding-agent/ — CLI 本体

`pi` 命令的实现，职责最杂：参数解析、会话管理、工具实现、交互界面、三种非交互模式、扩展系统。

| 子路径 | 用途 |
| --- | --- |
| `src/cli.ts`、`src/main.ts` | 可执行入口与启动流程 |
| `src/config.ts` | 资源路径解析（源码检出 / npm 安装 / 独立二进制三种形态） |
| `src/cli/` | 参数解析、登录/认证命令、模型列表、会话选择、项目信任、启动 UI |
| `src/core/` | 与界面无关的核心：`agent-session.ts`（会话与 agent 循环接线）、工具、压缩、扩展加载、设置、模型注册/解析、MCP 配置、信任、遥测、导出 HTML |
| `src/core/tools/` | 内置工具实现：`read`、`write`、`edit`、`bash`、`powershell`、`find`、`grep`、`ls`；`renderers/` 是各自的行内渲染 |
| `src/core/compaction/` | 上下文压缩与分支摘要 |
| `src/core/extensions/` | 扩展加载器与运行时（jiti 加载、wrapper、虚拟模块） |
| `src/core/export-html/` | 把会话导出成静态 HTML（模板与 ANSI→HTML 转换） |
| `src/modes/` | 运行模式：`interactive/`（TUI）、`print-mode.ts`、`json-event.ts`、`rpc/`（JSONL RPC 服务端） |
| `src/modes/interactive/` | 交互模式：主循环、聊天视口、主题、素材、`components/`（消息、选择器、编辑器、状态栏、diff、mermaid 等） |
| `src/extensions/` | 内置扩展：`codemode`（沙箱执行工具）、`mcp`（MCP 接入与 OAuth）、`tool-search`、`llama`（llama.cpp provider） |
| `src/utils/` | 通用工具：shell、git、图片、剪贴板、文件监听、进程、路径、版本检查等 |
| `src/bun/` | 单文件二进制（`bun build --compile`）的运行时引导 |
| `src/experimental/` | 实验性的 TS 源码解析器（用于生成 schema 与引用文档） |
| `docs/` | 用户文档：CLI、RPC、SDK、扩展、技能、MCP、模型配置、会话格式、主题等 |
| `examples/` | 扩展示例、SDK 示例、RPC 客户端示例 |
| `schemas/` | 配置 JSON Schema（settings、keybindings、models、theme） |
| `scripts/` | 生成 schema 与文档引用、会话迁移脚本 |
| `install-lock/` | 安装器专用的锁定 package.json / package-lock.json |
| `test/` | 单元测试；`test/suite/` 使用 harness + faux provider，`test/mcp-conformance/` 为 MCP 一致性测试 |

### packages/ai/ — 统一 LLM 接口

对接各家模型 API，统一消息、流式事件、模型目录、认证与成本统计。

| 子路径 | 用途 |
| --- | --- |
| `src/api/` | 各家协议实现（Anthropic messages、OpenAI responses/chat、Google Gemini/Vertex、Bedrock Converse、Mistral、Cloudflare 等），`*.lazy.ts` 为按需加载入口 |
| `src/providers/` | 各 provider 的注册与模型清单（`anthropic.ts`、`openai.ts`、`deepseek.ts`、`faux.ts` 等），`all.ts` 汇总 |
| `src/auth/` | 凭据解析、凭据存储、OAuth 流程（`auth/oauth/`） |
| `src/compat/` | 与扩展共享的 OAuth 类型等兼容层 |
| `src/utils/` | 重试、事件流、代理、错误体解析、token 估算、溢出判断、JSON 解析等 |
| `src/models.generated.ts` | 由 `scripts/generate-models.ts` 生成，禁止手改 |
| `scripts/` | 模型目录生成、模型数据校验、定价与推理参数抓取 |
| `src/cli.ts` | `pi-ai` 命令行入口 |

### packages/agent/ — 通用 agent 核心

与 coding-agent 无关的 agent 抽象：工具执行、事件流、状态管理、传输抽象。

- `src/agent.ts`：agent 状态与对外接口
- `src/agent-loop.ts`：模型请求 → 工具调用 → 结果回填的循环
- `src/proxy.ts`、`src/stream-fn.ts`：传输/流式函数抽象
- `src/types.ts`：共享类型
- `examples/mcp-codemode/`：把 MCP 工具接进 codemode 的示例
- `test/`：单元测试与 e2e

### packages/tui/ — 终端 UI 框架

基于 diff 渲染的 TUI 库，供 coding-agent 的交互模式使用。

- `src/tui.ts`、`src/tui-main-screen.ts`、`src/tui-alt-screen.ts`：主屏/备用屏渲染器
- `src/components/`：box、text、editor、markdown、select-list、scroll-view、image、loader 等基础组件
- `src/editor-component.ts`、`src/undo-stack.ts`、`src/kill-ring.ts`、`src/word-navigation.ts`：编辑器能力
- `src/keys.ts`、`src/keybindings.ts`：按键解析与可配置键位
- `src/terminal*.ts`、`src/stdin-buffer.ts`、`src/wheel-scroll.ts`：终端能力与输入
- `src/image.ts` 相关、`src/terminal-image.ts`：终端图片渲染
- `native/`：平台原生模块（剪贴板/终端能力），含 `darwin/`、`linux/`、`win32/` 的 C/ObjC 源码、构建脚本与 prebuilds

### packages/codemode/ — 模型代码沙箱

在 QuickJS（WASM）里执行模型写的 JavaScript，唯一能力是调用注入的工具；嵌套调用不进入 LLM 上下文。

- `src/runtime/`：执行、宿主桥、VM、worker、远程执行、协议与 prelude
- `src/wasm.ts`、`src/cloudflare.ts`、`src/portable.ts`：不同宿主的沙箱实现
- `src/declarations.ts`、`src/source.ts`：脚本环境声明与源码处理

### packages/mcp/ — MCP 客户端

不依赖官方 SDK 的独立 MCP 实现。

- `src/protocol/`：JSON-RPC 与 MCP 类型、内容块
- `src/transports/`：stdio、Streamable HTTP、内存传输
- `src/oauth/`：发现、授权流程、回调、provider
- `src/testing/`：测试辅助
- `LICENSES/`：取自 MCP TypeScript SDK 的许可证

### packages/chord/ — 多进程对象运行时

对象（状态 + 代码 + 地址）可跨进程引用、状态可复制、代码可热重载。是 `durable` 的底座。

- `src/contract.ts`、`src/declare.ts`、`src/schema.ts`、`src/types.ts`：契约与声明
- `src/host.ts`、`src/manager.ts`、`src/registry.ts`、`src/loader.ts`、`src/reload.ts`：宿主、进程管理与热重载
- `src/link.ts`、`src/connection.ts`、`src/websocket.ts`、`src/wire.ts`、`src/json.ts`：链路与线格式
- `src/delta/`：事务式 JSON 追踪、结构 diff、不可变应用
- `src/sqlite/`、`src/telemetry-sqlite/`：可移植 SQLite 适配与遥测存储
- `src/context/`、`src/output.ts`：取消/遥测上下文、有界输出与进度
- `docs/`：使用指南、规范、教程

### packages/durable/ — 持久化 agent 运行时

实验性。对话、模型轮次、工具调用在展示前先落盘，进程中断后可恢复。

- `src/harness/`：agent 定义、任务图、调度、压缩、生成、事件、inbox、provider
- `src/session/`：会话、事务、fork、观察
- `src/storage/`：存储实现（内存、JSONL、SQLite、IndexedDB）与扫描
- `src/tools/`：文件与图片工具（read/write/edit/bash/image 等）
- `src/tool-search/`、`src/codemode/`：工具检索与 codemode 集成
- `src/env/`：执行环境抽象及 Node 实现
- `src/images/`：图片处理（Node 与 Cloudflare）
- `src/testing/`：conformance/benchmark 辅助
- `test-workers/`：Cloudflare Workers/Durable Objects 下的测试
- `docs/spec.md`：规范文档

### packages/env/ — 远程执行环境

让 durable agent 的工具在另一台机器上跑（通常通过 SSH），本地仍保留存储与凭据。

- `daemon/`：Rust 程序 `pi-env`，在远端以 stdin/stdout 帧协议执行文件操作与命令（`src/` 下 `fs`、`exec`、`pty`、`watch`、`frame` 等）
- `src/`：`RemoteExecutionEnv` 客户端、SSH 连接、watch、远端文件列表
- `docs/`：协议与语义说明

### packages/telemetry/ — 遥测契约

厂商无关的 telemetry 接口：`TelemetryContext`/`TelemetrySpan`、noop 实现、内存实现、指标类型与 schema 工具、一致性测试。

### packages/evals/ — 行为评测

基于 vitest-evals 的 coding agent 评测。

- `evals/`：评测用例；`*.docs.eval.ts` 为文档提升评测（有/无文档对比），其余为宿主评测
- `src/`：CLI、docker 编排、harness、计划与报告
- `docker/`：评测容器镜像与入口

## scripts/（仓库级）

| 路径 | 用途 |
| --- | --- |
| `biome-check.mjs`、`biome/` | 格式化与 lint 包装、Biome grit 规则 |
| `check-*.mjs`、`check-*.ts` | 各类门禁：依赖固定、运行时依赖、相对导入、入口图、安装锁、浏览器 smoke、模型目录兼容 |
| `build-coding-agent-bundle.mjs`、`build-binaries.sh` | 打包与独立二进制构建 |
| `publish.mjs`、`release.mjs`、`release-packages.mjs`、`release-notes.mjs`、`local-release.mjs` | 发布与版本流程 |
| `pack-packages.mjs`、`package-artifacts.mjs`、`package-workspaces.mjs` | 打包与产物校验 |
| `generate-coding-agent-install-lock.mjs` | 生成安装器锁文件 |
| `sync-versions.js` | 同步各 workspace 版本 |
| `profile-coding-agent-node.mjs` | TUI/RPC 模式性能剖析 |
| `issues/` | 本地 issue 分诊工具（拉取 issue、用 durable agent 分类、Web UI 复核，数据存本地 `data/`） |

## 测试与约定

- 各包 `test/` 下放 vitest（`packages/tui` 用 `node:test`）测试；仓库根 `./test.sh` 跑全部非 e2e。
- `packages/coding-agent/test/suite/` 使用 harness + faux provider，不调用真实 provider。
- 模型数据 `packages/ai/src/providers/data/` 为生成产物且被 gitignore。
- 仅 `packages/*/src`、`packages/*/test`、`packages/coding-agent/examples` 受根 tsconfig 检查，需使用可擦除 TS 语法。
