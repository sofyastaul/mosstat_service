@echo off
setlocal
if not exist .venv\Scripts\activate.bat (
  echo .venv not found. Create venv first: python -m venv .venv
  exit /b 1
)
call .venv\Scripts\activate.bat
uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000
