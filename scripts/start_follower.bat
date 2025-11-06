@echo off
REM Start Follower Broker (Node 2) - Windows

echo ==========================================
echo Starting Follower Broker (Node 2)
echo ==========================================

REM Configuration (update these with actual IPs)
set FOLLOWER_HOST=0.0.0.0
set FOLLOWER_PORT=9093
set REDIS_HOST=localhost
set REDIS_PORT=6379

REM Navigate to project root
cd /d "%~dp0\.."

REM Activate virtual environment if it exists
if exist venv\Scripts\activate.bat (
    call venv\Scripts\activate.bat
)

REM Start follower broker
python -m follower_broker.follower --host %FOLLOWER_HOST% --port %FOLLOWER_PORT% --redis-host %REDIS_HOST% --redis-port %REDIS_PORT%

pause
