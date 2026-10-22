param([string]$Target = "", [switch]$Development)
$ErrorActionPreference = "Stop"
$taskArguments = @((Join-Path $PSScriptRoot "package_release.py"))
if ($Target) { $taskArguments += @("--target", $Target) }
if ($Development) { $taskArguments += "--development" }
& python @taskArguments
if ($LASTEXITCODE -ne 0) { throw "FastK release build failed ($LASTEXITCODE)" }
