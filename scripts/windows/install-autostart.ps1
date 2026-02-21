$ErrorActionPreference = "Stop"

$taskName = "AQUA_MSK_Server"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$rootPath = $root.Path
$startupScript = Join-Path $rootPath "scripts\windows\start-app.bat"

if (-not (Test-Path (Join-Path $rootPath "data\sessions"))) {
  New-Item -ItemType Directory -Path (Join-Path $rootPath "data\sessions") -Force | Out-Null
}

$argument = "/c `"$startupScript`""
$action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument $argument -WorkingDirectory $rootPath
$trigger = New-ScheduledTaskTrigger -AtLogOn
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries

Register-ScheduledTask `
  -TaskName $taskName `
  -Action $action `
  -Trigger $trigger `
  -Settings $settings `
  -Description "Run AQUA MSK server at user logon" `
  -Force | Out-Null

Write-Host "Scheduled task '$taskName' created."
Write-Host "To start now, run: schtasks /Run /TN $taskName"
