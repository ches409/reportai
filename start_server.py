#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
학원 보고서 자동 생성 시스템 서버 실행 스크립트
"""
import os
import sys
import subprocess
from pathlib import Path

def check_venv():
    """가상환경 확인"""
    venv_path = Path("venv")
    if not venv_path.exists():
        print("❌ 가상환경을 찾을 수 없습니다.")
        print("\n가상환경을 먼저 생성해주세요:")
        print("  python -m venv venv")
        print("  venv\\Scripts\\activate")
        print("  pip install -r requirements.txt")
        return False
    
    # Windows용 가상환경 Python 경로
    if sys.platform == "win32":
        python_path = venv_path / "Scripts" / "python.exe"
    else:
        python_path = venv_path / "bin" / "python"
    
    if not python_path.exists():
        print("❌ 가상환경 Python을 찾을 수 없습니다.")
        return False
    
    return True

def check_env_file():
    """환경 변수 파일 확인"""
    env_path = Path(".env")
    if not env_path.exists():
        print("⚠️  .env 파일이 없습니다.")
        print("환경 변수를 설정해주세요.")
        return False
    return True

def main():
    """메인 함수"""
    print("=" * 50)
    print("  학원 보고서 자동 생성 시스템 서버")
    print("=" * 50)
    print()
    
    # 현재 디렉토리로 이동
    os.chdir(Path(__file__).parent)
    
    # 가상환경 확인
    if not check_venv():
        input("\nEnter 키를 눌러 종료하세요...")
        sys.exit(1)
    
    # .env 파일 확인 (경고만)
    check_env_file()
    print()
    
    # 가상환경 Python 경로
    if sys.platform == "win32":
        python_path = Path("venv") / "Scripts" / "python.exe"
    else:
        python_path = Path("venv") / "bin" / "python"
    
    print("🚀 서버 시작 중...")
    print()
    print("서버가 시작되면 브라우저에서 다음 주소로 접속하세요:")
    print("  http://localhost:8000")
    print()
    print("서버를 중지하려면 Ctrl+C를 누르세요.")
    print("=" * 50)
    print()
    
    try:
        # 서버 실행
        subprocess.run([str(python_path), "app.py"], check=True)
    except KeyboardInterrupt:
        print("\n\n서버가 종료되었습니다.")
    except subprocess.CalledProcessError as e:
        print(f"\n❌ 서버 실행 중 오류 발생: {e}")
        input("\nEnter 키를 눌러 종료하세요...")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류: {e}")
        input("\nEnter 키를 눌러 종료하세요...")
        sys.exit(1)

if __name__ == "__main__":
    main()

