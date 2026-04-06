"""
API 8: Lambda - list_functions

목적: 계정의 모든 Lambda 함수 조회

사용법:
    python -m print_test.lambda_fn.list_functions
    또는
    uv run python -m print_test.lambda_fn.list_functions
"""

import sys
from pathlib import Path

from print_test.utils.environment import setup_environment
from print_test.utils.printer import StructuredPrinter

# 프로젝트 루트에서 import
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


printer = StructuredPrinter()


def test_lambda_list_functions(mock_responses):
    """
    API 8: Lambda 함수 목록 조회

    목적: 계정의 모든 Lambda 함수 조회
    """
    printer.print_header("API 8: Lambda 함수 목록 조회", "list_functions")

    response = mock_responses.LAMBDA_LIST_FUNCTIONS_RESPONSE

    printer.print_section("원본 응답 구조")
    printer.print_response(response)

    printer.print_section("파싱된 데이터")
    functions_info = []
    for func in response.get('Functions', []):
        info = {
            'FunctionName': func.get('FunctionName'),
            'Runtime': func.get('Runtime'),
            'Memory': f"{func.get('MemorySize')}MB",
            'Timeout': f"{func.get('Timeout')}s",
            'LastModified': str(func.get('LastModified')),
            'PackageType': func.get('PackageType'),
        }
        functions_info.append(info)

    printer.print_response({'functions': functions_info})

    printer.print_key_info({
        '총 함수 수': len(response.get('Functions', [])),
        '런타임 분포': list(set([f.get('Runtime') for f in response.get('Functions', [])])),
        '페이지네이션': 'NextMarker' in response and response['NextMarker'] is not None
    })

    printer.print_parsing_tips([
        "PackageType: 'Zip' 또는 'Image'",
        "LoggingConfig는 최신 함수에만 있음",
        "LastModified는 ISO 8601 문자열",
        "NextMarker로 페이지네이션 처리"
    ])


def main():
    """메인 실행 함수"""
    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 78 + "║")
    print("║" + "  API 8: Lambda - list_functions".center(78) + "║")
    print("║" + " " * 78 + "║")
    print("╚" + "=" * 78 + "╝")

    # 환경변수 세팅
    setup_environment()

    # Mock 응답 로드
    print("🔄 Mock 응답 로드 중...\n")

    if not mock_responses:
        print("❌ Mock 응답을 로드할 수 없습니다.")
        print("   test/fixtures/aws_responses.py 파일이 필요합니다.")
        return

    print("✓ Mock 응답 로드 완료\n")

    # API 테스트
    try:
        test_lambda_list_functions(mock_responses)
    except AttributeError as e:
        print(f"\n❌ Mock 응답 필드 오류: {e}")
        print(f"   test/fixtures/aws_responses.py 파일 구조를 확인하세요.")
        return
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return

    # 완료 메시지
    print("\n" + "=" * 80)
    print("✅ API 8 테스트 완료!")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
