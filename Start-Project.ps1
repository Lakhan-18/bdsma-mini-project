param([switch]$DashboardOnly, [double]$Speed = 14400)
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
$projectPython = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $projectPython)) {
    throw 'Run Setup.ps1 first to create the project Python environment.'
}
if ($DashboardOnly) {
    & $projectPython scripts/run_project.py --dashboard-only
} else {
    & $projectPython scripts/run_project.py --speed $Speed
}
if ($LASTEXITCODE -ne 0) { throw 'The project stopped. See the error above and outputs/runtime/pipeline.json.' }
