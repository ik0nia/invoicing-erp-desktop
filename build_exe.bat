@echo off
setlocal

cd /d "%~dp0"

where py >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Python launcher "py" was not found.
  echo Install Python for Windows first, then re-run this script.
  pause
  exit /b 1
)

if not exist ".venv\Scripts\python.exe" (
  echo Creating virtual environment...
  py -3 -m venv .venv
  if errorlevel 1 goto :fail
)

call ".venv\Scripts\activate.bat"
if errorlevel 1 goto :fail

echo Installing/updating dependencies...
python -m pip install --upgrade pip
if errorlevel 1 goto :fail

pip install -r requirements.txt pyinstaller
if errorlevel 1 goto :fail

echo Building executable...
pyinstaller --noconfirm --clean --onedir --windowed --name DesktopStockErpIntegration --hidden-import=pystray._win32 --collect-all pystray --collect-all PIL desktop_stock_erp_app.py
if errorlevel 1 goto :fail

echo.
echo Build complete.
echo EXE path:
echo %cd%\dist\DesktopStockErpIntegration\DesktopStockErpIntegration.exe
pause
exit /b 0

:fail
echo.
echo Build failed. Check errors above.
pause
exit /b 1
