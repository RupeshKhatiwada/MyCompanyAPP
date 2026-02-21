$ErrorActionPreference = "Stop"

$taskName = "AQUA_MSK_Server"
$task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($null -eq $task) {
  Write-Host "Scheduled task '$taskName' not found."
  exit 0
}

Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
Write-Host "Scheduled task '$taskName' removed."
