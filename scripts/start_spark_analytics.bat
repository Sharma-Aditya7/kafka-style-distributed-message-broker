@echo off
REM Start Spark Analytics Job - Windows

echo ==========================================
echo Starting Spark Flight Delay Analytics
echo ==========================================

REM Configuration
set BROKERS=localhost:9092 localhost:9093
set REDIS_HOST=localhost
set REDIS_PORT=6379
set CONSUMER_ID=spark-analytics-consumer
set OUTPUT=output\flight_analysis

REM Navigate to project root
cd /d "%~dp0\.."

REM Activate virtual environment if it exists
if exist venv\Scripts\activate.bat (
    call venv\Scripts\activate.bat
)

REM Run Spark job
echo Configuration:
echo   Brokers: %BROKERS%
echo   Redis: %REDIS_HOST%:%REDIS_PORT%
echo   Consumer ID: %CONSUMER_ID%
echo   Output: %OUTPUT%
echo.

python -m spark_jobs.flight_delay_streaming --brokers %BROKERS% --redis-host %REDIS_HOST% --redis-port %REDIS_PORT% --consumer-id %CONSUMER_ID% --output %OUTPUT% --save

pause
