@echo off
setlocal
if not exist .venv\Scripts\activate.bat (
  echo .venv not found. Create venv first: python -m venv .venv
  exit /b 1
)
call .venv\Scripts\activate.bat
streamlit run frontend/app.py
