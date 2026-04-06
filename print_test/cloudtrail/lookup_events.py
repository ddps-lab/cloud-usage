"""
API 5: CloudTrail - lookup_events

목적: EC2 인스턴스 상태 변화 이벤트 조회 (전체 활성 region)

사용법:
    python -m print_test.cloudtrail.lookup_events
    또는
    uv run python -m print_test.cloudtrail.lookup_events
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from pprint import pprint

import boto3

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from print_test.utils.printer import StructuredPrinter


printer = StructuredPrinter()

#SEARCH_MODES = ["RunInstances", "StartInstances", "TerminateInstances", "StopInstances", "BidEvictedEvent"]
SEARCH_MODES = ["RunInstances"]


def get_active_regions(session):
    """활성화된 AWS region 목록 반환"""
    ec2 = session.client('ec2')
    regions = [r['RegionName'] for r in ec2.describe_regions()['Regions']]
    return regions


def lookup_cloudtrail_events(cloudtrail, event_name, start_time, end_time):
    """
    CloudTrail lookup_events 호출 (NextToken 페이지네이션 포함)

    반환: 전체 Events 리스트 (모든 페이지 합산)
    """
    all_events = []
    next_token = None

    while True:
        kwargs = {
            'LookupAttributes': [{'AttributeKey': 'EventName', 'AttributeValue': event_name}],
            'StartTime': start_time,
            'EndTime': end_time,
        }
        if next_token:
            kwargs['NextToken'] = next_token

        response = cloudtrail.lookup_events(**kwargs)
        all_events.extend(response.get('Events', []))

        next_token = response.get('NextToken')
        if not next_token:
            break

    return all_events


def test_cloudtrail_lookup_events(region, event_name, events):
    """
    API 5: CloudTrail 이벤트 로그 조회 결과 출력

    목적: EC2 인스턴스 상태 변화 이벤트 조회
    """
    printer.print_header(f"API 5: CloudTrail [{region}] {event_name}", "lookup_events")

    printer.print_section("파싱된 데이터")
    events_info = []
    for event in events:
        # CloudTrailEvent는 JSON 문자열이므로 파싱 필수
        cloud_trail_event = json.loads(event.get('CloudTrailEvent', '{}'))
        print(f"raw trail event")
        pprint(cloud_trail_event)
        info = {
            'EventId': event.get('EventId'),
            'EventName': event.get('EventName'),
            'EventTime': str(event.get('EventTime')),
            'Username': event.get('Username'),
            'Resources': [r.get('ResourceName') for r in event.get('Resources', [])],
            'RequestParameters': cloud_trail_event.get('requestParameters', {})
        }
        events_info.append(info)

    printer.print_response({'events': events_info})

    printer.print_key_info({
        '총 이벤트 수': len(events),
        '이벤트 타입': list(set([e.get('EventName') for e in events])),
        '이벤트 시간': str(events[0].get('EventTime', 'N/A')) if events else 'N/A'
    })

    printer.print_parsing_tips([
        "CloudTrailEvent는 JSON 문자열 - json.loads() 필수",
        "EventTime은 ISO 8601 문자열 형식",
        "Resources 리스트가 비어있을 수 있음",
        "NextToken으로 페이지네이션 처리",
        "동일 이벤트가 중복될 수 있음 - 필터링 필요"
    ])


def main():
    """메인 실행 함수"""
    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 78 + "║")
    print("║" + "  API 5: CloudTrail - lookup_events (전체 region)".center(78) + "║")
    print("║" + " " * 78 + "║")
    print("╚" + "=" * 78 + "╝")

    profile = os.environ.get('AWS_PROFILE', 'default')
    print(f"🔄 AWS 세션 생성 중... (profile={profile})\n")

    session = boto3.Session(profile_name=profile)

    # 활성 region 목록 조회
    regions = get_active_regions(session)
    print(f"✓ 활성 region {len(regions)}개 탐색 완료: {regions}\n")

    # 시간 범위: 전일 KST 기준 (aws_daily_instance_usage_report.py 패턴 동일)
    utc_now = datetime.now(timezone.utc)
    kst = timezone(timedelta(hours=9))
    start_time = (utc_now + timedelta(days=-1)).astimezone(kst).replace(hour=0, minute=0, second=0, microsecond=0)
    end_time = utc_now.astimezone(kst).replace(hour=17, minute=0, second=0, microsecond=0)
    print(f"📅 조회 기간: {start_time.strftime('%Y-%m-%d %H:%M')} ~ {end_time.strftime('%Y-%m-%d %H:%M')} KST\n")

    # region별 × mode별 조회
    for region in regions:
        print(f"region: {region}")
        for mode in SEARCH_MODES:
            try:
                cloudtrail = session.client('cloudtrail', region_name=region)
                events = lookup_cloudtrail_events(cloudtrail, mode, start_time, end_time)

                if not events:
                    print(f"  [{region}] {mode}: 이벤트 없음")
                    continue

                test_cloudtrail_lookup_events(region, mode, events)

            except Exception as e:
                print(f"\n❌ [{region}] {mode} 오류: {e}")
                import traceback
                traceback.print_exc()

    # 완료 메시지
    print("\n" + "=" * 80)
    print("✅ API 5 테스트 완료!")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
