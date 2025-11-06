@echo off
REM Start Leader Broker (Node 1) - Windows

echo ==========================================
echo Starting Leader Broker (Node 1)
echo ==========================================

REM Configuration (update these with actual IPs)
set LEADER_HOST=0.0.0.0
set LEADER_PORT=9092
set FOLLOWER_HOST=localhost
set FOLLOWER_PORT=9093
set REDIS_HOST=localhost
set REDIS_PORT=6379

REM Navigate to project root
cd /d "%~dp0\.."

REM Activate virtual environment if it exists
if exist venv\Scripts\activate.bat (
    call venv\Scripts\activate.bat
)

REM Start leader broker
python -m leader_broker.leader --host %LEADER_HOST% --port %LEADER_PORT% --follower-host %FOLLOWER_HOST% --follower-port %FOLLOWER_PORT% --redis-host %REDIS_HOST% --redis-port %REDIS_PORT%

pause
