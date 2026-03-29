# ZhiWei Plugins

知微官方插件 monorepo。

仓库地址：

- `ZhiWei-plugins`: `https://github.com/ntygod/ZhiWei-plugins`
- `ZhiWei-index`: `https://github.com/ntygod/ZhiWei-index`

这个仓库用于维护：

- 官方渠道插件包
- 官方 connector 工程
- Marketplace 渠道索引导出工具

主系统运行时和 Marketplace 控制面仍然保留在主仓库 `ZhiWei` 中；这个仓库只负责插件源码和发布产物。

## 目录结构

```text
channels/
  feishu/
  wecom/
  dingtalk/

connectors/
  mock-http-connector/

tools/
  export-channel-index.ps1
```

## 当前内容

### 渠道插件

- `channels/feishu`
- `channels/wecom`
- `channels/dingtalk`

每个插件目录都包含：

- `channel-plugin.json`
- `docs/README.md`
- `examples/*.json`
- `assets/icon.svg`

### connector

- `connectors/mock-http-connector`

这是一个最小可运行的 HTTP connector 样板工程，用来验证知微当前的外部 connector 协议。

## 索引导出

将渠道插件导出为 Marketplace 可消费的 `CHANNEL` 条目：

```powershell
pwsh -NoProfile -File .\tools\export-channel-index.ps1 -OutputPath .\artifacts\index.json
```

如果要合并现有的 `ZhiWei-index`：

```powershell
pwsh -NoProfile -File .\tools\export-channel-index.ps1 `
  -ExistingIndexPath .\index.json `
  -OutputPath .\artifacts\index.merged.json
Move-Item .\artifacts\index.merged.json .\index.json -Force
```

## 自动发布索引

仓库内置了自动同步 `ZhiWei-index` 的工作流：

- `.github/workflows/publish-index.yml`

它会在 `channels/**` 或导出脚本变更后：

1. 导出渠道索引
2. 拉取 `ZhiWei-index`
3. 合并旧索引中的非渠道条目
4. 自动提交并推送新的 `index.json`

工作流依赖仓库 secret：

- `ZHIWEI_INDEX_PUSH_TOKEN`

## connector 开发

建议后续官方 connector 都从 `connectors/mock-http-connector` 复制起步：

- `feishu-connector`
- `wecom-connector`
- `dingtalk-connector`

然后再替换各自平台协议实现。

## 与主仓库的边界

`ZhiWei` 主仓库保留：

- 渠道控制面
- Marketplace 安装器
- manifest 校验
- 统一 runtime 协议

`ZhiWei-plugins` 保留：

- 插件包源码
- connector 工程
- 索引导出和发布脚本
