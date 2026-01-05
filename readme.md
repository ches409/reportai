# 서버 시작 가이드

# 1. 가상환경 실행
& C:/reportai/venv/Scripts/Activate.ps1
정상적으로 실행됐다면 터미널의 파일 경로 옆에 (venv)표시가 생성됩니다.

만약 실행되지 않는다면 
    ctrl + shift + p -> python:select interpreter -> python 3.11.9(venv)로 설정해주시고 다시 실행해주세요.

# 2. 라이브러리 설치
pip install -r requirements.txt

# 3. .env 파일에서 llm 모델 이름 체크
    OLLAMA_MODEL="qwen3:8b"

# 4. app.py 실행
vscode 편집기 오른쪽 위 run python file 버튼 클릭 또는
&C:/reportai/venv/Scripts/python.exe c:/reportai/app.py 

# 서버 에러

# 1. WeasyPrint 라이브러리 미설치
    gobject-2.0-0이 없다는 오류 문구가 떴다면 
    C:wsys62 폴더 및 msys2 실행 -> pacman -Syu 입력 -> 중간에 꺼졌다면 pacman -Su 입력 -> 
    pacman -S mingw-w64-x86_64-pango 
    pacman -S mingw-w64-x86_64-cairo 
    pacman -S mingw-w64-x86_64-gdk-pixbuf2 
    pacman -S mingw-w64-x86_64-glib2

    순서대로 입력해주세요.
    
    그리고 시스템 환경변수 편집 -> 환경변수 -> Path 설정에 C:\msys64\mingw64\bin 추가 해주세요.
    
    다시 서버를 실행해주세요.


# 노션 DB 입력, 수정, 삭제, 읽기 작업
https://www.notion.so/database-2d32f86d712c808fb36aeba2c43af21d
해당 url로 들어간 후 원하는 테이블에 들어가서 작업.
단, 읽기 권한만 있는 경우 입력, 수정, 삭제 기능이 제한됨.

데이터베이스 입력방법
import pandas as pd

df = pd.read_csv("11.csv", encoding="euc-kr")  
df.to_csv("notion_import3.csv", index=False, encoding="utf-8-sig")


# 보고서 요청 방법
- https://www.notion.so/2d32f86d712c802ca711cac666000c60?v=2d32f86d712c8079996c000c69c9cd3c
- 해당 url로 들어간 후 질문 셀에 질문 입력.

- 보고서 작성은 30초 간격마다 대기 중 상태에 있는 질문들을 대상으로 작성합니다.

- AI가 보고서를 이상하게 작성했을 경우 다시 대기 중 상태로 바꿔주세요.

- 보고서 작성은 20~30초가 소요됩니다.
