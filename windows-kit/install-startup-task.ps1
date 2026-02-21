# Run in PowerShell as current user
$ScriptPath = Join-Path $PSScriptRoot "start-aqua-msk-hidden.vbs"
$Action = New-ScheduledTaskAction -Execute "wscript.exe" -Argument ("\"" + $ScriptPath + "\"")
$Trigger = New-ScheduledTaskTrigger -AtLogOn
Register-ScheduledTask -TaskName "AQUA_MSK_AutoStart" -Action $Action -Trigger $Trigger -Description "Auto start AQUA MSK at user logon" -Force
Write-Host "Startup task installed: AQUA_MSK_AutoStart"
