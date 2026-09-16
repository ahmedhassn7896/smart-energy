# Test all project URLs and endpoints
$endpoints = @(
    @{ Name = "Kafka UI";           Url = "http://localhost:8080" },
    @{ Name = "Spark Master UI";    Url = "http://localhost:8090" },
    @{ Name = "Spark Worker UI";    Url = "http://localhost:8091" },
    @{ Name = "Spark History UI";   Url = "http://localhost:18080" },
    @{ Name = "Airflow Web UI";     Url = "http://localhost:8088" },
    @{ Name = "Airflow Flower";     Url = "http://localhost:5555" },
    @{ Name = "ClickHouse HTTP";    Url = "http://localhost:8123/ping" }
)

Write-Host "====================================================" -ForegroundColor Cyan
Write-Host " HTTP ENDPOINT AUDIT (from Windows host)" -ForegroundColor Cyan
Write-Host "====================================================" -ForegroundColor Cyan

foreach ($ep in $endpoints) {
    try {
        $res = Invoke-WebRequest -Uri $ep.Url -TimeoutSec 5 -UseBasicParsing -ErrorAction Stop
        Write-Host "[$($res.StatusCode)] $($ep.Name) ($($ep.Url)) - OK (Bytes: $($res.RawContentLength))" -ForegroundColor Green
    } catch {
        Write-Host "[FAIL] $($ep.Name) ($($ep.Url)) - Error: $($_.Exception.Message)" -ForegroundColor Red
    }
}
