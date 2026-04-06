"""
API 1: EC2 - describe_instances

목적: 모든 리전의 running/stopped 인스턴스 조회

사용법:
    python -m print_test.ec2.describe_instances
    또는
    uv run python -m print_test.ec2.describe_instances
"""

import os
import sys
from pathlib import Path

import boto3


# 프로젝트 루트에서 import
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from print_test.utils.printer import StructuredPrinter
from print_test.utils.environment import setup_environment



printer = StructuredPrinter()


def test_ec2_describe_instances(response):
    """
    API 1: EC2 인스턴스 조회

    목적: 모든 리전의 running/stopped 인스턴스 조회
    """
    printer.print_header("API 1: EC2 인스턴스 조회", "describe_instances")

    printer.print_section("원본 응답 구조")
    printer.print_response(response)

    printer.print_section("파싱된 데이터")
    instances_info = []
    for reservation in response.get('Reservations', []):
        for instance in reservation.get('Instances', []):
            info = {
                'InstanceId': instance.get('InstanceId'),
                'InstanceType': instance.get('InstanceType'),
                'State': instance.get('State', {}).get('Name'),
                'LaunchTime': str(instance.get('LaunchTime')),
                'Tags': {tag['Key']: tag['Value'] for tag in instance.get('Tags', [])}
            }
            instances_info.append(info)

    printer.print_response({'instances': instances_info})

    printer.print_key_info({
        '총 인스턴스 수': len(instances_info),
        'Running': len([i for i in instances_info if i['State'] == 'running']),
        'Stopped': len([i for i in instances_info if i['State'] == 'stopped']),
    })

    printer.print_parsing_tips([
        "Reservations 리스트가 빈 경우 처리 필요",
        "Tags 필드는 선택사항 - .get() 사용",
        "State.Name으로 상태 확인 (running, stopped, terminated)",
        "LaunchTime은 datetime 객체 - 문자열로 변환 후 출력"
    ])


def main():
    """메인 실행 함수"""
    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 78 + "║")
    print("║" + "  API 1: EC2 - describe_instances".center(78) + "║")
    print("║" + " " * 78 + "║")
    print("╚" + "=" * 78 + "╝")

    # 환경변수 세팅
    setup_environment()

    # AWS 세션 및 클라이언트 생성
    profile = os.environ.get('AWS_PROFILE', 'default')
    print(f"🔄 AWS API 호출 중... (profile={profile}, 전체 리전)\n")

    session = boto3.Session(profile_name=profile)

    # 활성화된 리전 목록 조회
    ec2_global = session.client('ec2', region_name='us-east-1')
    regions_response = ec2_global.describe_regions(
        Filters=[{'Name': 'opt-in-status', 'Values': ['opt-in-not-required', 'opted-in']}]
    )
    regions = [r['RegionName'] for r in regions_response['Regions']]
    print(f"  조회 대상 리전 수: {len(regions)}")

    # 전체 리전 집계
    all_reservations = []
    for region in regions:
        try:
            ec2 = session.client('ec2', region_name=region)
            resp = ec2.describe_instances()
            for reservation in resp.get('Reservations', []):
                for instance in reservation.get('Instances', []):
                    instance['_Region'] = region
                all_reservations.append(reservation)
        except Exception as e:
            print(f"  [{region}] 조회 실패: {e}")

    response = {'Reservations': all_reservations}
    print("✓ API 응답 수신 완료\n")

    # API 테스트
    try:
        test_ec2_describe_instances(response)
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return

    # 완료 메시지
    print("\n" + "=" * 80)
    print("✅ API 1 테스트 완료!")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
