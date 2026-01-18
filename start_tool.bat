@echo off
REM Start the MHP CRM Tool
REM This launches the Flask server with the interactive map (Crexi/LoopNet leads + Pipeline)

echo Starting MHP CRM Tool...
echo.
echo The map will be available at: http://localhost:8000
echo.
echo Features:
echo   - Interactive map with all leads
echo   - Crexi and LoopNet lead markers
echo   - Pipeline management
echo   - Tax shock scores, insurance scores, flood zones
echo.
echo Press Ctrl+C to stop the server
echo.

cd execution
python crm_server.py
