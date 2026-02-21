param(
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
