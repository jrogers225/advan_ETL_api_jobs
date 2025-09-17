@echo off
REM Batch file to run load_media_plan\main.py
REM This can be scheduled in Windows Task Scheduler
REM Usage: run_media_plan_load.bat [start_date]
REM Example: run_media_plan_load.bat 2025-08-01
REM Set default start date to 365 days ago if no parameter provided

set START_DATE=%1
if "%START_DATE%"=="" (
    REM Calculate date 365 days ago
    powershell -Command "& {(Get-Date).AddDays(-365).ToString('yyyy-MM-dd')}" > temp_date.txt
    set /p START_DATE=<temp_date.txt
    del temp_date.txt
)

echo Using start date: %START_DATE%

REM Set the working directory to the project root
cd /d "C:\Users\jrogers\Documents\Visual Studio Code\Python Projects\advan_ETL_api_jobs-main"

REM Log file for tracking execution - replace colons with underscores for valid filename
set TIMESTAMP=%time:~0,2%_%time:~3,2%_%time:~6,2%
set TIMESTAMP=%TIMESTAMP::=_%
set TIMESTAMP=%TIMESTAMP: =0%
set LOGFILE=logs\media_plan_load_%date:~-4,4%%date:~-10,2%%date:~-7,2%_%TIMESTAMP%.log

REM Create logs directory if it doesn't exist
if not exist logs mkdir logs

REM Prune old log files - keep only the 10 most recent files
echo Pruning old log files...
forfiles /p logs /m media_plan_load_*.log /c "cmd /c echo Deleting old log: @path" 2>nul
for /f "skip=10 delims=" %%i in ('dir /b /o-d logs\media_plan_load_*.log 2^>nul') do (
    echo Deleting old log file: logs\%%i
    del /q "logs\%%i"
)

REM Run the Python script using the virtual environment
echo Starting Media Plan Load at %date% %time% with start date %START_DATE% >> %LOGFILE% 2>&1
"%~dp0.venv\Scripts\python.exe" "%~dp0load_media_plan\main.py" --start-date %START_DATE% >> %LOGFILE% 2>&1

REM Capture the exit code before any other operations
set SCRIPT_EXIT_CODE=%ERRORLEVEL%

REM Check exit code and log result
if %SCRIPT_EXIT_CODE% EQU 0 (
    echo Media Plan Load completed successfully at %date% %time% >> %LOGFILE% 2>&1
    echo Script finished with exit code 0 >> %LOGFILE% 2>&1
) else (
    echo Media Plan Load failed with error code %SCRIPT_EXIT_CODE% at %date% %time% >> %LOGFILE% 2>&1
    echo Script finished with exit code %SCRIPT_EXIT_CODE% >> %LOGFILE% 2>&1
)

REM Clean exit - ensure Task Scheduler recognizes completion
echo Batch file exiting cleanly >> %LOGFILE% 2>&1
echo End of batch execution at %date% %time% >> %LOGFILE% 2>&1

REM Force close any lingering file handles and ensure clean termination
if exist temp_date.txt del temp_date.txt 2>nul
set LOGFILE=
set START_DATE=
set SCRIPT_EXIT_CODE=

REM Explicit exit with proper code
exit /b %SCRIPT_EXIT_CODE%