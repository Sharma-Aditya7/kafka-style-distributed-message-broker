@echo off
REM Start Consumer Client (Node 4) - Windows

echo ==========================================
echo Starting Consumer Client (Node 4)
echo ==========================================

REM Configuration (update with actual broker IPs)
set CONSUMER_ID=consumer-1
set BROKERS=172.23.51.66:9092 172.23.202.101:9093
set REDIS_HOST=172.23.202.101
set REDIS_PORT=6379

REM Navigate to project root
cd /d "%~dp0\.."

REM Activate virtual environment if it exists
if exist venv\Scripts\activate.bat (
    call venv\Scripts\activate.bat
)

REM Start consumer
python -m consumer.consumer --consumer-id %CONSUMER_ID% --brokers %BROKERS% --redis-host %REDIS_HOST% --redis-port %REDIS_PORT%

pause
