const fs = require("fs");
const path = require("path");

const projectRoot = path.join(__dirname, "..", "..");
const windowsKitDir = path.join(projectRoot, "windows-kit");

const writeFile = (filename, content) => {
  fs.writeFileSync(path.join(windowsKitDir, filename), content, "utf8");
};

const buildWindowsKit = () => {
  fs.mkdirSync(windowsKitDir, { recursive: true });

  writeFile("start-aqua-msk.ps1", `$ErrorActionPreference = "Stop"
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
$proc = Start-Process -FilePath $NodeCmd.Source -ArgumentList "src\\server.js" -WorkingDirectory $AppRoot -WindowStyle Hidden -RedirectStandardOutput $OutLog -RedirectStandardError $ErrLog -PassThru
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
`);

  writeFile("stop-aqua-msk.ps1", `$ErrorActionPreference = "SilentlyContinue"
$AppRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$DataDir = Join-Path $AppRoot "data"
$PidFile = Join-Path $DataDir "aqua-msk.pid"
$killed = 0

if (Test-Path $PidFile) {
  $pidText = (Get-Content $PidFile -ErrorAction SilentlyContinue | Select-Object -First 1)
  $pidValue = 0
  if ([int]::TryParse($pidText, [ref]$pidValue)) {
    $proc = Get-Process -Id $pidValue -ErrorAction SilentlyContinue
    if ($proc) {
      Stop-Process -Id $pidValue -Force
      $killed++
    }
  }
  Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
}

$appPathToken = [regex]::Escape((Resolve-Path $AppRoot).Path)
Get-CimInstance Win32_Process -Filter "Name='node.exe'" | Where-Object {
  $_.CommandLine -and $_.CommandLine -match "src\\\\server\\.js" -and $_.CommandLine -match $appPathToken
} | ForEach-Object {
  Stop-Process -Id $_.ProcessId -Force
  $killed++
}

Write-Host "AQUA MSK server stopped. Processes killed: $killed"
`);

  writeFile("start-aqua-msk.bat", `@echo off
setlocal
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start-aqua-msk.ps1"
if errorlevel 1 (
  echo Failed to start AQUA MSK.
  pause
  exit /b 1
)
exit /b 0
`);

  writeFile("start-aqua-msk-hidden.vbs", `Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")
scriptPath = fso.GetParentFolderName(WScript.ScriptFullName) & "\\start-aqua-msk.ps1"
shell.Run "powershell -NoProfile -ExecutionPolicy Bypass -File """ & scriptPath & """", 0, False
`);

  writeFile("stop-aqua-msk.bat", `@echo off
setlocal
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0stop-aqua-msk.ps1"
if /I "%~1"=="--no-pause" exit /b 0
echo.
pause
`);

  writeFile("restart-aqua-msk.bat", `@echo off
setlocal
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0stop-aqua-msk.ps1"
ping 127.0.0.1 -n 3 >nul
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start-aqua-msk.ps1"
if errorlevel 1 (
  echo Restart failed.
  pause
  exit /b 1
)
exit /b 0
`);

  writeFile("install-startup-task.ps1", `$ErrorActionPreference = "Stop"
$TaskName = "AQUA_MSK_AutoStart"
$ScriptPath = Join-Path $PSScriptRoot "start-aqua-msk-hidden.vbs"
$Action = New-ScheduledTaskAction -Execute "wscript.exe" -Argument ('"' + $ScriptPath + '"')
$Trigger = New-ScheduledTaskTrigger -AtLogOn
$Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings -Description "Auto start AQUA MSK at user logon" -Force | Out-Null

$Desktop = [Environment]::GetFolderPath("Desktop")
$Shell = New-Object -ComObject WScript.Shell

$StartShortcut = $Shell.CreateShortcut((Join-Path $Desktop "AQUA MSK.lnk"))
$StartShortcut.TargetPath = Join-Path $PSScriptRoot "start-aqua-msk-hidden.vbs"
$StartShortcut.WorkingDirectory = Split-Path (Join-Path $PSScriptRoot "start-aqua-msk-hidden.vbs")
$StartShortcut.Save()

$StopShortcut = $Shell.CreateShortcut((Join-Path $Desktop "AQUA MSK - Stop.lnk"))
$StopShortcut.TargetPath = Join-Path $PSScriptRoot "stop-aqua-msk.bat"
$StopShortcut.WorkingDirectory = Split-Path (Join-Path $PSScriptRoot "stop-aqua-msk.bat")
$StopShortcut.Save()

Write-Host "Startup task installed and desktop shortcuts created."
`);

  writeFile("remove-startup-task.ps1", `$ErrorActionPreference = "SilentlyContinue"
Unregister-ScheduledTask -TaskName "AQUA_MSK_AutoStart" -Confirm:$false
Write-Host "Startup task removed."
`);

  writeFile("install-staff-pc.ps1", `$ErrorActionPreference = "Stop"
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
`);

  writeFile("maintenance-one-click.bat", `@echo off
setlocal
set APP_DIR=%~dp0..
cd /d "%APP_DIR%"
if not exist data\\sessions mkdir data\\sessions
if not exist data\\backups mkdir data\\backups
if not exist data\\logs mkdir data\\logs
call npm run deploy:ready
if errorlevel 1 (
  echo Maintenance failed.
  pause
  exit /b 1
)
echo Maintenance completed successfully.
pause
`);

  writeFile("update-aqua-msk.bat", `@echo off
setlocal
set APP_DIR=%~dp0..
cd /d "%APP_DIR%"
if not exist data\\sessions mkdir data\\sessions
if not exist data\\backups mkdir data\\backups
if not exist data\\logs mkdir data\\logs

call windows-kit\\stop-aqua-msk.bat --no-pause

where git >nul 2>&1
if %errorlevel%==0 (
  echo Pulling latest changes...
  git pull
)

call npm install --no-fund --no-audit
if errorlevel 1 (
  echo npm install failed.
  pause
  exit /b 1
)

call npm run deploy:ready
if errorlevel 1 (
  echo Update failed.
  pause
  exit /b 1
)

call windows-kit\\start-aqua-msk.bat
echo Update completed successfully.
pause
`);

  writeFile("backup-to-usb.ps1", `param(
  [Parameter(Mandatory=$true)]
  [string]$TargetPath
)

$AppRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$DataPath = Join-Path $AppRoot "data"
$BackupPath = Join-Path $DataPath "backups"
$Stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$OutDir = Join-Path $TargetPath ("AQUA_MSK_BACKUP_" + $Stamp)
New-Item -ItemType Directory -Path $OutDir -Force | Out-Null

if (Test-Path (Join-Path $DataPath "aqua.db")) {
  Copy-Item (Join-Path $DataPath "aqua.db") (Join-Path $OutDir "aqua.db") -Force
}
if (Test-Path (Join-Path $DataPath "aqua.db-shm")) {
  Copy-Item (Join-Path $DataPath "aqua.db-shm") (Join-Path $OutDir "aqua.db-shm") -Force
}
if (Test-Path (Join-Path $DataPath "aqua.db-wal")) {
  Copy-Item (Join-Path $DataPath "aqua.db-wal") (Join-Path $OutDir "aqua.db-wal") -Force
}
if (Test-Path $BackupPath) {
  Copy-Item $BackupPath (Join-Path $OutDir "backups") -Recurse -Force
}
Write-Host "Backup copied to $OutDir"
`);

  writeFile("README_WINDOWS.txt", `AQUA MSK Windows Production Package
===================================

Goal: staff run AQUA MSK with one click, no terminal.

First-time setup on each staff PC:
1) Install Node.js 20 LTS.
2) Open PowerShell in this windows-kit folder.
3) Run:
   powershell -ExecutionPolicy Bypass -File .\\install-staff-pc.ps1

Daily staff use:
- Double click desktop shortcut: "AQUA MSK"

Auto-start on login:
- Installed automatically by install-staff-pc.ps1
- Task name: AQUA_MSK_AutoStart

Manual controls:
- Start: start-aqua-msk.bat
- Stop: stop-aqua-msk.bat
- Restart: restart-aqua-msk.bat

Maintenance:
- maintenance-one-click.bat

Update package:
- update-aqua-msk.bat

Backup to USB:
- powershell -ExecutionPolicy Bypass -File .\\backup-to-usb.ps1 -TargetPath E:\\
`);

  const files = fs.readdirSync(windowsKitDir).sort();
  return { windowsKitDir, files };
};

module.exports = {
  buildWindowsKit,
  windowsKitDir
};
