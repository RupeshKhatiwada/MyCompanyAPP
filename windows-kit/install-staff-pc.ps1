$ErrorActionPreference = "Stop"
$KitRoot = Resolve-Path $PSScriptRoot
$AppRoot = Resolve-Path (Join-Path $KitRoot "..")
Set-Location $AppRoot

$NodeCmd = Get-Command node -ErrorAction SilentlyContinue
if (-not $NodeCmd) {
  Write-Host "Node.js not found. Install Node.js 20 LTS first."
  exit 1
}

$VersionText = (& node -v).Trim().TrimStart('v')
$Major = [int]($VersionText.Split('.')[0])
if ($Major -lt 20) {
  Write-Host "Node.js 20+ is required. Current version: $VersionText"
  exit 1
}

if (-not (Test-Path (Join-Path $AppRoot "node_modules"))) {
  Write-Host "Installing dependencies..."
  npm install --no-fund --no-audit
  if ($LASTEXITCODE -ne 0) {
    Write-Host "npm install failed."
    exit 1
  }
}

Write-Host "Building CSS..."
npm run build:css
if ($LASTEXITCODE -ne 0) {
  Write-Host "build:css failed."
  exit 1
}

Write-Host "Installing auto-start task and desktop shortcut..."
powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $KitRoot "install-startup-task.ps1")
if ($LASTEXITCODE -ne 0) {
  Write-Host "Startup task installation failed."
  exit 1
}

Write-Host "Starting AQUA MSK..."
powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $KitRoot "start-aqua-msk.ps1")
if ($LASTEXITCODE -ne 0) {
  Write-Host "AQUA MSK start failed."
  exit 1
}

Write-Host "Done. Staff can now launch with desktop icon: AQUA MSK"
