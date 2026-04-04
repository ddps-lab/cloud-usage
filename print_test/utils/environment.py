"""환경변수 설정 및 로드"""
import os
from pathlib import Path
from typing import Dict


# 프로젝트 루트: print_test/utils/environment.py → 상위 2단계
_PROJECT_ROOT = Path(__file__).parent.parent.parent


def load_env_from_file(env_file_path: str = ".env") -> Dict[str, str]:
    """
    .env 파일에서 환경변수 로드

    형식:
        KEY = "value"
        또는
        KEY="value"

    반환:
        {KEY: value} 딕셔너리
    """
    env_vars = {}
    if not os.path.exists(env_file_path):
        print(f"⚠️  {env_file_path} 파일이 없습니다. 환경변수 생략")
        return env_vars

    with open(env_file_path, 'r') as f:
        for line in f:
            line = line.strip()
            # 주석, 빈 줄 제외
            if not line or line.startswith('#'):
                continue
            # KEY = "value" 형식 파싱
            if '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                env_vars[key] = value

    return env_vars


def setup_environment():
    """
    환경변수 세팅 (구체적인 단계)

    단계 1: .env 파일에서 로드
    단계 2: OS 환경변수에 설정
    단계 3: AWS_PROFILE 설정 (AWS CLI 기반 자격증명)
    단계 4: AWS 리전 설정
    """
    print("=" * 80)
    print("🔧 환경변수 세팅")
    print("=" * 80)

    # 단계 1: .env 파일 로드 (프로젝트 루트 기준 절대경로)
    env_file_path = str(_PROJECT_ROOT / ".env")
    env_vars = load_env_from_file(env_file_path)
    print(f"\n✓ {env_file_path}에서 로드된 변수:")
    for key, value in env_vars.items():
        masked_value = value[:20] + "..." if len(value) > 20 else value
        print(f"  - {key}: {masked_value}")

    # 단계 2: OS 환경변수에 설정
    for key, value in env_vars.items():
        os.environ[key] = value
        print(f"  ✓ os.environ['{key}'] 설정됨")

    # 단계 3: AWS_PROFILE 설정 (AWS 자격증명 기반)
    if 'AWS_PROFILE' not in os.environ:
        os.environ['AWS_PROFILE'] = 'default'
        print(f"\n✓ AWS_PROFILE 기본값 설정:")
        print(f"  - AWS_PROFILE: default (기본 프로필)")
    else:
        print(f"\n✓ AWS_PROFILE 설정:")
        print(f"  - AWS_PROFILE: {os.environ['AWS_PROFILE']}")

    # 단계 4: AWS 리전 설정
    if 'AWS_REGION' not in os.environ:
        os.environ['AWS_REGION'] = 'ap-northeast-2'
    if 'AWS_DEFAULT_REGION' not in os.environ:
        os.environ['AWS_DEFAULT_REGION'] = os.environ['AWS_REGION']

    print(f"\n✓ AWS 리전 설정:")
    print(f"  - AWS_REGION: {os.environ['AWS_REGION']}")
    print(f"  - AWS_DEFAULT_REGION: {os.environ['AWS_DEFAULT_REGION']}")

    print("\n" + "=" * 80 + "\n")
