# Channel Packages

这个目录存放知微官方渠道插件包。

当前包含：

- `feishu`
- `wecom`
- `dingtalk`

每个插件包至少包含：

- `channel-plugin.json`
- `docs/README.md`
- `examples/*.json`
- `assets/icon.svg`

Marketplace 安装器会把这些目录视为插件发布单元，并根据 `channel-plugin.json` 拉取 README、图标和示例配置。
