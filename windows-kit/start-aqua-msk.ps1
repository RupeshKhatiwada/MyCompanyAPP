$ErrorActionPreference = "Stop"
$AppRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $AppRoot

$DataDir = Join-Path $AppRoot "data"
$SessionDir = Join-Path $DataDir "sessions"
$BackupDir = Join-Path $DataDir "backups"
$LogDir = Join-Path $DataDir "logs"
$PidFile = Join-Path $DataDir "aqua-msk.pid"
$HealthUrl = "http://localhost:3000/health"
$AppUrl = "http://localhost:3000"

New-Item -ItemType Directory -Path $SessionDir -Force | Out-Null
New-Item -ItemType Directory -Path $BackupDir -Force | Out-Null
New-Item -ItemType Directory -Path $LogDir -Force | Out-Null

function Test-ServerHealth {
  try {
    $response = Invoke-WebRequest -Uri $HealthUrl -UseBasicParsing -TimeoutSec 2
    return ($response.StatusCode -ge 200 -and $response.StatusCode -lt 500)
  } catch {
    return $false
  }
}

if (Test-ServerHealth) {
  Start-Process $AppUrl | Out-Null
  exit 0
}

$NodeCmd = Get-Command node -ErrorAction SilentlyContinue
if (-not $NodeCmd) {
  Write-Host "Node.js is not installed. Install Node.js 20 LTS and run install-staff-pc.ps1 first."
  exit 1
}

$OutLog = Join-Path $LogDir "server.out.log"
$ErrLog = Join-Path $LogDir "server.err.log"
$proc = Start-Process -FilePath $NodeCmd.Source -ArgumentList "src\server.js" -WorkingDirectory $AppRoot -WindowStyle Hidden -RedirectStandardOutput $OutLog -RedirectStandardError $ErrLog -PassThru
Set-Content -Path $PidFile -Value $proc.Id -Encoding ascii -Force

for ($i = 0; $i -lt 40; $i++) {
  Start-Sleep -Milliseconds 500
  if (Test-ServerHealth) {
    Start-Process $AppUrl | Out-Null
    exit 0
  }
}

Start-Process $AppUrl | Out-Null
exit 0
