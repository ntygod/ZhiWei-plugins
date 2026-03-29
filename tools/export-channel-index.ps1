param(
    [string]$PluginRoot = "channels",
    [string]$RepoBaseUrl = "https://raw.githubusercontent.com/ntygod/ZhiWei-plugins/main/channels",
    [string]$OutputPath = "artifacts/index.json",
    [string]$MinLifepilotVersion = "1.0.0",
    [string]$DefaultAuthor = "zhiwei-official",
    [string]$ExistingIndexPath = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-FallbackTimestamp {
    return (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
}

function Get-GitTimestamp {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    try {
        $value = git log -1 --format=%cI -- $Path 2>$null
        if ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace($value)) {
            return $value.Trim()
        }
    } catch {
    }

    return Get-FallbackTimestamp
}

function Get-PluginDescription {
    param(
        [string]$ReadmePath,
        [Parameter(Mandatory = $true)]
        [string]$PluginName
    )

    if (-not $ReadmePath -or -not (Test-Path $ReadmePath)) {
        return "知微官方$PluginName渠道插件。"
    }

    foreach ($line in (Get-Content -Path $ReadmePath)) {
        $trimmed = $line.Trim()
        if ([string]::IsNullOrWhiteSpace($trimmed)) {
            continue
        }
        if ($trimmed.StartsWith("#")) {
            continue
        }
        if ($trimmed.StartsWith("- ")) {
            continue
        }
        return $trimmed
    }

    return "知微官方$PluginName渠道插件。"
}

function Get-PluginTags {
    param(
        [Parameter(Mandatory = $true)]
        [pscustomobject]$Manifest
    )

    $tags = [System.Collections.Generic.List[string]]::new()
    $tags.Add("channel")

    if (-not [string]::IsNullOrWhiteSpace([string]$Manifest.platform)) {
        $tags.Add(([string]$Manifest.platform).Trim())
    }

    if (-not [string]::IsNullOrWhiteSpace([string]$Manifest.connectorMode)) {
        $tags.Add(([string]$Manifest.connectorMode).Trim().ToLowerInvariant())
    }

    return @($tags | Select-Object -Unique)
}

function Get-PluginRequirements {
    param(
        [Parameter(Mandatory = $true)]
        [pscustomobject]$Manifest
    )

    $requirements = [System.Collections.Generic.List[string]]::new()

    if ([string]$Manifest.connectorMode -eq "EXTERNAL") {
        $requirements.Add("需要外部 connector")
    }

    if ([string]$Manifest.connectorMode -eq "LOCAL") {
        $requirements.Add("主服务内建渠道")
    }

    if ($null -ne $Manifest.connectorSpec -and -not [string]::IsNullOrWhiteSpace([string]$Manifest.connectorSpec.protocol)) {
        $requirements.Add("connector 协议：" + ([string]$Manifest.connectorSpec.protocol).Trim())
    }

    return @($requirements | Select-Object -Unique)
}

function Get-ReadmePath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PluginDir,
        [Parameter(Mandatory = $true)]
        [pscustomobject]$Manifest
    )

    if ($null -eq $Manifest.resources) {
        return $null
    }
    if ([string]::IsNullOrWhiteSpace([string]$Manifest.resources.readmePath)) {
        return $null
    }

    return Join-Path $PluginDir ([string]$Manifest.resources.readmePath)
}

function Convert-PluginToIndexEntry {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PluginDir,
        [Parameter(Mandatory = $true)]
        [pscustomobject]$Manifest
    )

    if ([string]::IsNullOrWhiteSpace([string]$Manifest.pluginId)) {
        throw "插件清单缺少 pluginId: $PluginDir"
    }
    if ([string]::IsNullOrWhiteSpace([string]$Manifest.name)) {
        throw "插件清单缺少 name: $PluginDir"
    }
    if ([string]::IsNullOrWhiteSpace([string]$Manifest.version)) {
        throw "插件清单缺少 version: $PluginDir"
    }

    $pluginId = ([string]$Manifest.pluginId).Trim()
    $pluginName = ([string]$Manifest.name).Trim()
    $author = ([string]$Manifest.vendor).Trim()
    if ([string]::IsNullOrWhiteSpace($author)) {
        $author = $DefaultAuthor
    }

    $timestamp = Get-GitTimestamp -Path $PluginDir
    $readmePath = Get-ReadmePath -PluginDir $PluginDir -Manifest $Manifest

    return [ordered]@{
        id = $pluginId
        name = $pluginName
        type = "CHANNEL"
        version = ([string]$Manifest.version).Trim()
        author = $author
        description = Get-PluginDescription -ReadmePath $readmePath -PluginName $pluginName
        repoUrl = ($RepoBaseUrl.TrimEnd("/") + "/" + $pluginId)
        filePath = "channel-plugin.json"
        tags = @(Get-PluginTags -Manifest $Manifest)
        requirements = @(Get-PluginRequirements -Manifest $Manifest)
        minLifepilotVersion = $MinLifepilotVersion
        createdAt = $timestamp
        updatedAt = $timestamp
        downloads = 0
        verified = $true
    }
}

function Read-ExistingEntries {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (-not (Test-Path $Path)) {
        return @()
    }

    $raw = Get-Content -Path $Path -Raw
    if ([string]::IsNullOrWhiteSpace($raw)) {
        return @()
    }

    return @(ConvertFrom-Json -InputObject $raw)
}

$pluginRootPath = Resolve-Path $PluginRoot
$pluginDirs = Get-ChildItem -Path $pluginRootPath -Directory | Sort-Object Name
$channelEntries = [System.Collections.Generic.List[object]]::new()

foreach ($pluginDir in $pluginDirs) {
    $manifestPath = Join-Path $pluginDir.FullName "channel-plugin.json"
    if (-not (Test-Path $manifestPath)) {
        continue
    }

    $manifest = Get-Content -Path $manifestPath -Raw | ConvertFrom-Json
    $channelEntries.Add((Convert-PluginToIndexEntry -PluginDir $pluginDir.FullName -Manifest $manifest))
}

$finalEntries = @($channelEntries)
if (-not [string]::IsNullOrWhiteSpace($ExistingIndexPath)) {
    $existingEntries = Read-ExistingEntries -Path $ExistingIndexPath
    $nonChannelEntries = @($existingEntries | Where-Object { $_.type -ne "CHANNEL" })
    $finalEntries = @($nonChannelEntries + $channelEntries)
}

$outputDir = Split-Path -Parent $OutputPath
if (-not [string]::IsNullOrWhiteSpace($outputDir) -and -not (Test-Path $outputDir)) {
    New-Item -ItemType Directory -Path $outputDir -Force | Out-Null
}

$finalEntries | ConvertTo-Json -Depth 12 | Set-Content -Path $OutputPath -Encoding UTF8
Write-Output ("已生成渠道索引: " + (Resolve-Path $OutputPath))
