$ErrorActionPreference = "Stop"
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
