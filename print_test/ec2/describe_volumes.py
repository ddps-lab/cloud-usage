"""
API 2: EC2 - describe_volumes

목적: 미사용 EBS 볼륨 조회

사용법:
    python -m print_test.ec2.describe_volumes
    또는
    uv run python -m print_test.ec2.describe_volumes
"""

import os
import sys
from pathlib import Path

import boto3

# 프로젝트 루트에서 import
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from print_test.utils.environment import setup_environment
from print_test.utils.printer import StructuredPrinter


printer = StructuredPrinter()


def test_ec2_describe_volumes(response):
    """
    API 2: EC2 미사용 볼륨 조회

    목적: 미사용 EBS 볼륨 조회
    """
    printer.print_header("API 2: EC2 미사용 볼륨 조회", "describe_volumes")

    printer.print_section("원본 응답 구조")
    printer.print_response(response)

    printer.print_section("파싱된 데이터")
    volumes_info = []
    for volume in response.get('Volumes', []):
        info = {
            'VolumeId': volume.get('VolumeId'),
            'Size': f"{volume.get('Size')}GB",
            'Type': volume.get('VolumeType'),
            'Status': volume.get('State'),
            'CreatedTime': str(volume.get('CreateTime')),
            'Tags': {tag['Key']: tag['Value'] for tag in volume.get('Tags', [])}
        }
        volumes_info.append(info)

    printer.print_response({'volumes': volumes_info})

    printer.print_key_info({
        '총 볼륨 수': len(volumes_info),
        '총 용량': f"{sum(float(v['Size'].replace('GB', '')) for v in volumes_info if 'GB' in v['Size'])}GB",
        'Volume 타입': list(set([v['Type'] for v in volumes_info]))
    })

    printer.print_parsing_tips([
        "Status='available'은 미사용 볼륨",
        "CreateTime은 datetime 객체",
        "Kubernetes 태그로 cluster 리소스 구분",
        "Size는 정수형 - GB 단위"
    ])


def main():
    """메인 실행 함수"""
    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 78 + "║")
    print("║" + "  API 2: EC2 - describe_volumes".center(78) + "║")
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
    all_volumes = []
    for region in regions:
        try:
            ec2 = session.client('ec2', region_name=region)
            resp = ec2.describe_volumes()
            for volume in resp.get('Volumes', []):
                volume['_Region'] = region
                all_volumes.append(volume)
        except Exception as e:
            print(f"  [{region}] 조회 실패: {e}")

    response = {'Volumes': all_volumes}
    print("✓ API 응답 수신 완료\n")

    # API 테스트
    try:
        test_ec2_describe_volumes(response)
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return

    # 완료 메시지
    print("\n" + "=" * 80)
    print("✅ API 2 테스트 완료!")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
