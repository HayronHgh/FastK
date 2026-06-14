param(
    [string]$Target = ""
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

function Get-CommandOutput {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string[]]$Arguments
    )

    $output = & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$FilePath $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
    }
    return ($output | Out-String).Trim()
}

function Get-Sha256 {
    param([Parameter(Mandatory = $true)][string]$Path)

    return (Get-FileHash -Algorithm SHA256 -LiteralPath $Path).Hash.ToLowerInvariant()
}

function Get-RelativeManifestPath {
    param(
        [Parameter(Mandatory = $true)][string]$BaseDir,
        [Parameter(Mandatory = $true)][string]$Path
    )

    $baseFull = (Resolve-Path -LiteralPath $BaseDir).Path
    $pathFull = (Resolve-Path -LiteralPath $Path).Path
    $separator = [System.IO.Path]::DirectorySeparatorChar
    if (-not $baseFull.EndsWith([string]$separator)) {
        $baseFull = "$baseFull$separator"
    }
    $baseUri = New-Object System.Uri($baseFull)
    $pathUri = New-Object System.Uri($pathFull)
    $relative = [System.Uri]::UnescapeDataString($baseUri.MakeRelativeUri($pathUri).ToString())
    return ($relative -replace "\\", "/")
}

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..")
Set-Location $RepoRoot

$metadataRaw = Get-CommandOutput "cargo" @("metadata", "--no-deps", "--format-version", "1")
$metadata = $metadataRaw | ConvertFrom-Json
$package = $metadata.packages | Where-Object { $_.name -eq "fastk" } | Select-Object -First 1
if ($null -eq $package) {
    throw "fastk package not found in cargo metadata"
}
$version = [string]$package.version

$rustcVerbose = Get-CommandOutput "rustc" @("-vV")
$hostLine = ($rustcVerbose -split "`n") | Where-Object { $_ -like "host:*" } | Select-Object -First 1
if (-not $hostLine) {
    throw "unable to determine rustc host target"
}
$hostTarget = $hostLine.Substring(5).Trim()
$effectiveTarget = if ([string]::IsNullOrWhiteSpace($Target)) { $hostTarget } else { $Target }

$gitCommit = "unknown"
$previousErrorActionPreference = $ErrorActionPreference
$ErrorActionPreference = "Continue"
try {
    $gitOutput = & git rev-parse --short=12 HEAD 2>$null
    if ($LASTEXITCODE -eq 0 -and $gitOutput) {
        $gitCommit = ($gitOutput | Out-String).Trim()
    }
}
finally {
    $ErrorActionPreference = $previousErrorActionPreference
}

$buildArgs = @("build", "--release", "--examples")
if (-not [string]::IsNullOrWhiteSpace($Target)) {
    $buildArgs += @("--target", $Target)
}
Invoke-Checked "cargo" $buildArgs
Invoke-Checked "cargo" @("package", "--allow-dirty", "--no-verify")

$exeExt = if ($effectiveTarget -like "*windows*") { ".exe" } else { "" }
$releaseDir = if ([string]::IsNullOrWhiteSpace($Target)) {
    Join-Path $RepoRoot "target\release\examples"
} else {
    Join-Path $RepoRoot "target\$Target\release\examples"
}

$distRoot = Join-Path $RepoRoot "dist"
$packageDir = Join-Path $distRoot "fastk-$version-$effectiveTarget"
if (Test-Path -LiteralPath $packageDir) {
    Remove-Item -LiteralPath $packageDir -Recurse -Force
}

$binDir = Join-Path $packageDir "bin"
$crateDir = Join-Path $packageDir "crate"
$docsDir = Join-Path $packageDir "docs"
$schemasDir = Join-Path $packageDir "schemas"
New-Item -ItemType Directory -Force -Path $binDir, $crateDir, $docsDir, $schemasDir | Out-Null

$bridgeSource = Join-Path $releaseDir "fastk_bridge$exeExt"
$adminSource = Join-Path $releaseDir "fastk_admin$exeExt"
if (-not (Test-Path -LiteralPath $bridgeSource)) {
    throw "missing built fastk_bridge binary: $bridgeSource"
}
if (-not (Test-Path -LiteralPath $adminSource)) {
    throw "missing built fastk_admin binary: $adminSource"
}

$bridgeDest = Join-Path $binDir "fastk_bridge$exeExt"
$adminDest = Join-Path $binDir "fastk_admin$exeExt"
Copy-Item -LiteralPath $bridgeSource -Destination $bridgeDest
Copy-Item -LiteralPath $adminSource -Destination $adminDest

$crateSource = Join-Path $RepoRoot "target\package\fastk-$version.crate"
if (-not (Test-Path -LiteralPath $crateSource)) {
    throw "missing packaged crate: $crateSource"
}
$crateDest = Join-Path $crateDir "fastk-$version.crate"
Copy-Item -LiteralPath $crateSource -Destination $crateDest

$licenseSource = Join-Path $RepoRoot "LICENSE"
if (-not (Test-Path -LiteralPath $licenseSource)) {
    throw "missing release license: LICENSE"
}
$licenseDest = Join-Path $packageDir "LICENSE"
Copy-Item -LiteralPath $licenseSource -Destination $licenseDest

$docs = @(
    "README.md",
    "docs\ARCHITECTURE_BOUNDARY.md",
    "docs\STORE_LIFECYCLE.md",
    "docs\BACKTEST_INTEGRATION.md",
    "docs\REPLAY_AND_TAIL.md",
    "docs\RELEASE_CHECKLIST.md",
    "docs\RELEASE_NOTES.md",
    "docs\BACKEND_INTEGRATION.md",
    "docs\BRIDGE_CONTRACT.md",
    "docs\KLINE_STORAGE_COMPARISON.md",
    "docs\PROJECT_STRUCTURE.md",
    "docs\SIGNAL_SCALAR_STORAGE.md"
)
foreach ($doc in $docs) {
    $source = Join-Path $RepoRoot $doc
    if (-not (Test-Path -LiteralPath $source)) {
        throw "missing release doc: $doc"
    }
    Copy-Item -LiteralPath $source -Destination (Join-Path $docsDir (Split-Path -Leaf $doc))
}

Get-ChildItem -LiteralPath (Join-Path $RepoRoot "schemas") -File | ForEach-Object {
    Copy-Item -LiteralPath $_.FullName -Destination (Join-Path $schemasDir $_.Name)
}

$artifactEntries = @()
foreach ($binary in @($bridgeDest, $adminDest)) {
    $artifactEntries += [ordered]@{
        path = Get-RelativeManifestPath $packageDir $binary
        kind = "binary"
        sha256 = Get-Sha256 $binary
    }
}
$artifactEntries += [ordered]@{
    path = Get-RelativeManifestPath $packageDir $crateDest
    kind = "crate"
    sha256 = Get-Sha256 $crateDest
}
$artifactEntries += [ordered]@{
    path = Get-RelativeManifestPath $packageDir $licenseDest
    kind = "license"
    sha256 = Get-Sha256 $licenseDest
}
Get-ChildItem -LiteralPath $docsDir -File | Sort-Object Name | ForEach-Object {
    $artifactEntries += [ordered]@{
        path = Get-RelativeManifestPath $packageDir $_.FullName
        kind = "document"
        sha256 = Get-Sha256 $_.FullName
    }
}
Get-ChildItem -LiteralPath $schemasDir -File | Sort-Object Name | ForEach-Object {
    $artifactEntries += [ordered]@{
        path = Get-RelativeManifestPath $packageDir $_.FullName
        kind = "schema"
        sha256 = Get-Sha256 $_.FullName
    }
}

$manifest = [ordered]@{
    name = "fastk"
    version = $version
    target = $effectiveTarget
    git_commit = $gitCommit
    rustc = $rustcVerbose
    cargo = Get-CommandOutput "cargo" @("--version")
    build_time_utc = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    artifacts = $artifactEntries
    stable_surfaces = @(
        "FastKStore",
        "BacktestStoreView",
        "KlineRecord",
        "ScalarRecord",
        "DatasetRegistry",
        "DatasetRef",
        "fastk_bridge JSON contract"
    )
    experimental_surfaces = @(
        "TradeRecord",
        "BboRecord",
        "BookDeltaRecord",
        "ReplayCursor",
        "SequenceScanReport",
        "day/hour partition internals"
    )
}

$manifestPath = Join-Path $packageDir "release_manifest.json"
$manifest | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $manifestPath -Encoding ascii

$checksumPath = Join-Path $packageDir "SHA256SUMS"
$sumLines = Get-ChildItem -LiteralPath $packageDir -File -Recurse |
    Where-Object { $_.Name -ne "SHA256SUMS" } |
    Sort-Object FullName |
    ForEach-Object {
        $rel = Get-RelativeManifestPath $packageDir $_.FullName
        "$(Get-Sha256 $_.FullName)  $rel"
    }
$sumLines | Set-Content -LiteralPath $checksumPath -Encoding ascii

Write-Host "release package: $packageDir"
Write-Host "manifest: $manifestPath"
Write-Host "checksums: $checksumPath"
