# ==============================================================================
# Smart Energy Consumption Analytics - Project URL Opener
# Automatically launches all active project services in default web browser
# ==============================================================================

Write-Host "`n========================================================" -ForegroundColor Cyan
Write-Host " SMART ENERGY CONSUMPTION ANALYTICS - WEB INTERFACES" -ForegroundColor Green
Write-Host "========================================================`n" -ForegroundColor Cyan

$urls = @(
    @{ Name = "Kafka UI";           Url = "http://localhost:8080";  Port = 8080 },
    @{ Name = "Spark Master UI";    Url = "http://localhost:8090";  Port = 8090 },
    @{ Name = "Spark Worker UI";    Url = "http://localhost:8091";  Port = 8091 },
    @{ Name = "Spark History UI";   Url = "http://localhost:18080"; Port = 18080 },
    @{ Name = "Airflow Web UI";     Url = "http://localhost:8088";  Port = 8088 },
    @{ Name = "Airflow Flower";     Url = "http://localhost:5555";  Port = 5555 },
    @{ Name = "ClickHouse HTTP";    Url = "http://localhost:8123";  Port = 8123 }
)

foreach ($service in $urls) {
    Write-Host "Testing connection to $($service.Name) on port $($service.Port)... " -NoNewline
    $tcp = Test-NetConnection -ComputerName "localhost" -Port $service.Port -WarningAction SilentlyContinue -InformationLevel Quiet
    if ($tcp) {
        Write-Host "[ONLINE] Opening $($service.Url)" -ForegroundColor Green
        Start-Process $service.Url
    } else {
        Write-Host "[OFFLINE] $($service.Url)" -ForegroundColor Yellow
    }
}

Write-Host "`nAll active services have been opened in your default browser.`n" -ForegroundColor Green
