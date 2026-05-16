param(
    [string]$Config = "configs/cluster.json"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$binary = Join-Path $root "bin\quorumdb.exe"
$logs = Join-Path $root "logs"

New-Item -ItemType Directory -Force -Path (Split-Path -Parent $binary) | Out-Null
New-Item -ItemType Directory -Force -Path $logs | Out-Null

Push-Location $root
try {
    go build -o $binary .\cmd\quorumdb

    foreach ($node in @("node1", "node2", "node3", "node4", "node5")) {
        $outLog = Join-Path $logs "$node.out.log"
        $errLog = Join-Path $logs "$node.err.log"
        Start-Process -FilePath $binary `
            -ArgumentList @("-node", $node, "-config", $Config) `
            -RedirectStandardOutput $outLog `
            -RedirectStandardError $errLog `
            -WindowStyle Hidden
    }

    Write-Host "Started QuorumDB nodes on ports 8001-8005"
    Write-Host "Try: Invoke-RestMethod -Method Put http://127.0.0.1:8001/kv/name -Body '{""value"":""quorumdb""}' -ContentType 'application/json'"
    Write-Host "Then: Invoke-RestMethod http://127.0.0.1:8004/kv/name"
}
finally {
    Pop-Location
}
