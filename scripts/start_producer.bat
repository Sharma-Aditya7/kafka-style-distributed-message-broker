@echo off
REM Start Producer Client (Node 3) - Windows

echo ==========================================
echo Starting Producer Client (Node 3)
echo ==========================================

REM Configuration (update with actual broker IPs)
set BROKERS=localhost:9092 localhost:9093
set REDIS_HOST=localhost
set REDIS_PORT=6379

REM Navigate to project root
cd /d "%~dp0\.."

REM Activate virtual environment if it exists
if exist venv\Scripts\activate.bat (
    call venv\Scripts\activate.bat
)

REM Start producer
python -m producer.producer --brokers %BROKERS% --redis-host %REDIS_HOST% --redis-port %REDIS_PORT%

pause
