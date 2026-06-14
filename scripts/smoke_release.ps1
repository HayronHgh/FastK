param(
    [string]$PackageDir = ""
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string[]]$Arguments
    )

    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$FilePath $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
    }
}

function Invoke-Capture {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string[]]$Arguments
    )

    $output = & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$FilePath $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
    }
    return ($output | Out-String)
}

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..")
Set-Location $RepoRoot

if ([string]::IsNullOrWhiteSpace($PackageDir)) {
    $metadataRaw = & cargo metadata --no-deps --format-version 1
    if ($LASTEXITCODE -ne 0) {
        throw "cargo metadata failed with exit code $LASTEXITCODE"
    }
    $metadata = ($metadataRaw | Out-String) | ConvertFrom-Json
    $package = $metadata.packages | Where-Object { $_.name -eq "fastk" } | Select-Object -First 1
    if ($null -eq $package) {
        throw "fastk package not found in cargo metadata"
    }
    $version = [string]$package.version
    $rustcVerbose = (& rustc -vV) -join "`n"
    if ($LASTEXITCODE -ne 0) {
        throw "rustc -vV failed with exit code $LASTEXITCODE"
    }
    $hostLine = ($rustcVerbose -split "`n") | Where-Object { $_ -like "host:*" } | Select-Object -First 1
    $hostTarget = $hostLine.Substring(5).Trim()
    $candidate = Join-Path $RepoRoot "dist\fastk-$version-$hostTarget"
    if (Test-Path -LiteralPath $candidate) {
        $PackageDir = $candidate
    } else {
        $matches = Get-ChildItem -LiteralPath (Join-Path $RepoRoot "dist") -Directory -ErrorAction SilentlyContinue |
            Where-Object { $_.Name -like "fastk-$version-*" } |
            Sort-Object LastWriteTime -Descending
        if ($matches.Count -eq 0) {
            throw "release package not found under dist; run scripts\build_release.ps1 first"
        }
        $PackageDir = $matches[0].FullName
    }
}

$PackageDir = (Resolve-Path $PackageDir).Path
$bridge = Join-Path $PackageDir "bin\fastk_bridge.exe"
$admin = Join-Path $PackageDir "bin\fastk_admin.exe"
if (-not (Test-Path -LiteralPath $bridge)) {
    $bridge = Join-Path $PackageDir "bin\fastk_bridge"
}
if (-not (Test-Path -LiteralPath $admin)) {
    $admin = Join-Path $PackageDir "bin\fastk_admin"
}
if (-not (Test-Path -LiteralPath $bridge)) {
    throw "missing fastk_bridge in release package: $PackageDir"
}
if (-not (Test-Path -LiteralPath $admin)) {
    throw "missing fastk_admin in release package: $PackageDir"
}

$store = Join-Path $RepoRoot "target\release-smoke-store"
$inputs = Join-Path $RepoRoot "target\release-smoke-input"
if (Test-Path -LiteralPath $store) {
    Remove-Item -LiteralPath $store -Recurse -Force
}
if (Test-Path -LiteralPath $inputs) {
    Remove-Item -LiteralPath $inputs -Recurse -Force
}
New-Item -ItemType Directory -Force -Path $store, $inputs | Out-Null

$previousErrorActionPreference = $ErrorActionPreference
$ErrorActionPreference = "Continue"
try {
    & $bridge --help 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw "fastk_bridge --help failed with exit code $LASTEXITCODE"
    }
}
finally {
    $ErrorActionPreference = $previousErrorActionPreference
}

Invoke-Checked $admin @("validate", "--root", $store, "--verbose")

$klineJson = Join-Path $inputs "kline.json"
@'
{
  "timeframe_ms": 60000,
  "price_scale": 100000,
  "volume_scale": 100000,
  "records": [
    { "ts": 1706745600000, "open": 10000000, "high": 10020000, "low": 9980000, "close": 10010000, "volume": 10000 },
    { "ts": 1706745660000, "open": 10010000, "high": 10030000, "low": 10000000, "close": 10020000, "volume": 10020 },
    { "ts": 1706745720000, "open": 10020000, "high": 10040000, "low": 10010000, "close": 10030000, "volume": 10030 }
  ]
}
'@ | Set-Content -LiteralPath $klineJson -Encoding ascii

$writeKline = Invoke-Capture $bridge @(
    "write-kline-range",
    "--root", $store,
    "--symbol", "BTCUSDT",
    "--timeframe", "1m",
    "--input-json", $klineJson
) | ConvertFrom-Json
if ($writeKline.written_record_count -ne 3) {
    throw "expected 3 written kline rows, got $($writeKline.written_record_count)"
}

$readKline = Invoke-Capture $bridge @(
    "read-kline-range",
    "--root", $store,
    "--symbol", "BTCUSDT",
    "--timeframe", "1m",
    "--start-ts", "1706745600000",
    "--end-ts", "1706745720000"
) | ConvertFrom-Json
if ($readKline.records.Count -ne 3) {
    throw "expected 3 read kline rows, got $($readKline.records.Count)"
}

$scalarJson = Join-Path $inputs "scalar.json"
@'
{
  "timeframe_ms": 60000,
  "records": [
    { "ts": 1706745600000, "value": 42 },
    { "ts": 1706745660000, "value": 43 },
    { "ts": 1706745720000, "value": 44 }
  ]
}
'@ | Set-Content -LiteralPath $scalarJson -Encoding ascii

$writeScalar = Invoke-Capture $bridge @(
    "write-scalar-range",
    "--root", $store,
    "--symbol", "BTCUSDT",
    "--timeframe", "1m",
    "--category", "feature",
    "--name", "rsi_14",
    "--input-json", $scalarJson
) | ConvertFrom-Json
if ($writeScalar.written_record_count -ne 3) {
    throw "expected 3 written scalar rows, got $($writeScalar.written_record_count)"
}

$readScalar = Invoke-Capture $bridge @(
    "read-scalar-range",
    "--root", $store,
    "--symbol", "BTCUSDT",
    "--timeframe", "1m",
    "--category", "feature",
    "--name", "rsi_14",
    "--start-ts", "1706745600000",
    "--end-ts", "1706745720000"
) | ConvertFrom-Json
if ($readScalar.records.Count -ne 3) {
    throw "expected 3 read scalar rows, got $($readScalar.records.Count)"
}

$predicateScalar = Invoke-Capture $bridge @(
    "query-scalar-predicate",
    "--root", $store,
    "--symbol", "BTCUSDT",
    "--timeframe", "1m",
    "--category", "feature",
    "--name", "rsi_14",
    "--start-ts", "1706745600000",
    "--end-ts", "1706745720000",
    "--predicate", "gt",
    "--value", "42",
    "--return-values"
) | ConvertFrom-Json
if ($predicateScalar.matches.Count -ne 2) {
    throw "expected 2 scalar predicate matches, got $($predicateScalar.matches.Count)"
}

Invoke-Checked $admin @("validate", "--root", $store, "--verbose")
Invoke-Checked $admin @("scrub", "--root", $store, "--verbose", "--dry-run")

Write-Host "smoke passed"
