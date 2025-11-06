@echo off
REM Start Flight Data Analytics Consumer (Node 4) - Windows

echo ==========================================
echo Starting Flight Data Analytics Consumer
echo ==========================================

REM Configuration
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

REM Run consumer in batch mode
echo Configuration:
echo   Consumer ID: %CONSUMER_ID%
echo   Brokers: %BROKERS%
echo   Redis: %REDIS_HOST%:%REDIS_PORT%
echo   Mode: Batch Analytics
echo.

python -m consumer.flight_data_consumer --consumer-id %CONSUMER_ID% --brokers %BROKERS% --redis-host %REDIS_HOST% --redis-port %REDIS_PORT% --batch

pause
