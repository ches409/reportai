@echo off
chcp 65001 >nul
title 학원 보고서 자동 생성 시스템 서버
color 0A

echo ========================================
echo   학원 보고서 자동 생성 시스템 서버
echo ========================================
echo.

REM 현재 디렉토리 확인
cd /d "%~dp0"

REM 가상환경 확인
if not exist "venv\Scripts\activate.bat" (
    echo [오류] 가상환경을 찾을 수 없습니다.
    echo 가상환경을 먼저 생성해주세요.
    echo.
    echo 다음 명령어를 실행하세요:
    echo   python -m venv venv
    echo   venv\Scripts\activate
    echo   pip install -r requirements.txt
    echo.
    pause
    exit /b 1
)

REM 가상환경 활성화
echo [1/3] 가상환경 활성화 중...
call venv\Scripts\activate.bat
if errorlevel 1 (
    echo [오류] 가상환경 활성화 실패
    pause
    exit /b 1
)

REM .env 파일 확인
if not exist ".env" (
    echo [경고] .env 파일이 없습니다.
    echo 환경 변수를 설정해주세요.
    echo.
)

REM 서버 실행
echo [2/3] 서버 시작 중...
echo.
echo 서버가 시작되면 브라우저에서 다음 주소로 접속하세요:
echo   http://localhost:8000
echo.
echo 서버를 중지하려면 Ctrl+C를 누르세요.
echo ========================================
echo.

python app.py

REM 서버 종료 후
echo.
echo 서버가 종료되었습니다.
pause


