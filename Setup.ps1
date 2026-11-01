param([switch]$CheckOnly)
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
$projectPython = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'

if (-not (Test-Path -LiteralPath $projectPython)) {
    & py -3.11 -m venv .venv
    if ($LASTEXITCODE -ne 0) { throw 'Python 3.11 environment creation failed.' }
}

if (-not $CheckOnly) {
    & $projectPython -m pip install --index-url https://pypi.org/simple -r requirements.txt --timeout 30 --retries 1
    if ($LASTEXITCODE -ne 0) {
        Write-Warning 'Package installation failed. Keep the error output for diagnosis.'
    }
}

# JAVA_HOME applies only to this terminal and its child processes.
$javaCandidates = @()
if ($env:JAVA_HOME) { $javaCandidates += $env:JAVA_HOME }
foreach ($javaRoot in @('C:\Program Files\Eclipse Adoptium', 'C:\Program Files\Java', 'C:\Program Files\Microsoft')) {
    if (Test-Path -LiteralPath $javaRoot) {
        $javaCandidates += Get-ChildItem -LiteralPath $javaRoot -Directory | Select-Object -ExpandProperty FullName
    }
}
$foundJava17 = $false
foreach ($candidate in ($javaCandidates | Select-Object -Unique)) {
    $releaseFile = Join-Path $candidate 'release'
    if ((Test-Path -LiteralPath $releaseFile) -and
        (Select-String -LiteralPath $releaseFile -Pattern '^JAVA_VERSION="17[.\"]' -Quiet)) {
        $env:JAVA_HOME = $candidate
        $env:PATH = (Join-Path $candidate 'bin') + ';' + $env:PATH
        $foundJava17 = $true
        Write-Host "Using Java 17: $candidate"
        break
    }
}
if (-not $foundJava17) {
    Write-Warning 'Java 17 was not found in the standard install locations. Install Temurin JDK 17 (Windows x64) from https://adoptium.net/temurin/releases/?version=17 and rerun this script.'
}
& $projectPython scripts/check_setup.py
if ($LASTEXITCODE -ne 0) { throw 'Setup checks failed.' }
Write-Host 'Next: open Docker Desktop, then run docker compose up -d from this folder.'
