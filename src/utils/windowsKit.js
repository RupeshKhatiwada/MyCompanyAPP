const fs = require("fs");
const path = require("path");

const projectRoot = path.join(__dirname, "..", "..");
const windowsKitDir = path.join(projectRoot, "windows-kit");

const writeFile = (filename, content) => {
  fs.writeFileSync(path.join(windowsKitDir, filename), content, "utf8");
};

const buildWindowsKit = () => {
  fs.mkdirSync(windowsKitDir, { recursive: true });

  writeFile("start-aqua-msk.bat", `@echo off
setlocal
set APP_DIR=%~dp0..
cd /d "%APP_DIR%"
if not exist data\\sessions mkdir data\\sessions
if not exist data\\backups mkdir data\\backups
start "" http://localhost:3000
node src\\server.js
`);

  writeFile("start-aqua-msk-hidden.vbs", `Set WshShell = CreateObject("WScript.Shell")
WshShell.Run chr(34) & ".\\start-aqua-msk.bat" & Chr(34), 0
Set WshShell = Nothing
`);

  writeFile("stop-aqua-msk.bat", `@echo off
taskkill /F /IM node.exe >nul 2>&1
echo AQUA MSK server stopped.
pause
`);

  writeFile("install-startup-task.ps1", `# Run in PowerShell as current user
$ScriptPath = Join-Path $PSScriptRoot "start-aqua-msk-hidden.vbs"
$Action = New-ScheduledTaskAction -Execute "wscript.exe" -Argument ("\\"" + $ScriptPath + "\\"")
$Trigger = New-ScheduledTaskTrigger -AtLogOn
Register-ScheduledTask -TaskName "AQUA_MSK_AutoStart" -Action $Action -Trigger $Trigger -Description "Auto start AQUA MSK at user logon" -Force
Write-Host "Startup task installed: AQUA_MSK_AutoStart"
`);

  writeFile("remove-startup-task.ps1", `Unregister-ScheduledTask -TaskName "AQUA_MSK_AutoStart" -Confirm:$false
Write-Host "Startup task removed."
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
if (Test-Path $BackupPath) {
  Copy-Item $BackupPath (Join-Path $OutDir "backups") -Recurse -Force
}
Write-Host "Backup copied to $OutDir"
`);

  writeFile("README_WINDOWS.txt", `AQUA MSK Windows Deployment Kit
================================

1) Place this "windows-kit" folder inside your AQUA MSK project root.
2) Double click: start-aqua-msk.bat
3) Open browser: http://localhost:3000

Auto start when Windows logs in:
- Right click PowerShell and Run:
  powershell -ExecutionPolicy Bypass -File .\\install-startup-task.ps1

Stop server:
- Double click: stop-aqua-msk.bat

Backup to USB drive:
- Example:
  powershell -ExecutionPolicy Bypass -File .\\backup-to-usb.ps1 -TargetPath E:\\
`);

  const files = fs.readdirSync(windowsKitDir).sort();
  return { windowsKitDir, files };
};

module.exports = {
  buildWindowsKit,
  windowsKitDir
};
