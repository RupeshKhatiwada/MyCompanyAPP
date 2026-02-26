$ErrorActionPreference = "SilentlyContinue"
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
  $_.CommandLine -and $_.CommandLine -match "src\\server\.js" -and $_.CommandLine -match $appPathToken
} | ForEach-Object {
  Stop-Process -Id $_.ProcessId -Force
  $killed++
}

Write-Host "AQUA MSK server stopped. Processes killed: $killed"
