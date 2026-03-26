@echo off
chcp 65001 >nul
cd /d "%~dp0"
pip install -r requirements.txt --quiet
python run_and_mail.py
pause
