@echo off
REM Start Flight Data Producer (Node 3) - Windows

echo ==========================================
echo Starting Flight Data Producer
echo ==========================================

REM Configuration
set BROKERS=:9092 172.23.159.119:9093
set REDIS_HOST=172.23.159.119
set REDIS_PORT=6379
set CSV_FILE=data\FlightDelay2.csv
set DELAY=100
set MAX_RECORDS=

REM Navigate to project root
cd /d "%~dp0\.."

REM Activate virtual environment if it exists
if exist venv\Scripts\activate.bat (
    call venv\Scripts\activate.bat
)

REM Run producer
echo Configuration:
echo   CSV File: %CSV_FILE%
echo   Brokers: %BROKERS%
echo   Redis: %REDIS_HOST%:%REDIS_PORT%
echo   Delay: %DELAY%ms
echo.

python -m producer.flight_data_producer --csv %CSV_FILE% --brokers %BROKERS% --redis-host %REDIS_HOST% --redis-port %REDIS_PORT% --delay %DELAY%

pause
