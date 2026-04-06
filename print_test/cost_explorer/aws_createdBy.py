"""
API 7: Cost Explorer - aws:createdBy 태그 기반 IAM 유저별 비용 집계

목적: aws:createdBy 태그를 dimension으로 활용해 계정 내 IAM 유저별 서비스 비용 집계 가능 여부 확인

사전 조건:
    - AWS Billing 콘솔 > Cost allocation tags 에서 'aws:createdBy' 활성화 필요
    - 활성화 후 최대 24시간 이후부터 Cost Explorer에서 조회 가능

확인 항목:
    1. TAG(aws:createdBy) 단일 GroupBy  — IAM 유저별 월간 총 비용 (MTD)
    2. 특정 유저 필터링                  — 단일 IAM 유저의 서비스별 월간 비용 (MTD)

응답 구조 주의:
    - Keys[0] 포맷: "aws:createdBy$<tag_value>"  (달러($)로 키 prefix 구분)
    - 미태깅 리소스: Keys[0] = "aws:createdBy$"  (value가 빈 문자열)
    - tag_value 추출:  group['Keys'][0].split('$', 1)[1]

사용법:
    python -m print_test.cost_explorer.aws_createdBy
    또는
    uv run python -m print_test.cost_explorer.aws_createdBy
"""

import os
import sys
from datetime import date
from pathlib import Path
import boto3

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from print_test.utils.environment import setup_environment
from print_test.utils.printer import StructuredPrinter

printer = StructuredPrinter()


# ─────────────────────────────────────────
# 날짜 계산
# ─────────────────────────────────────────

def build_month_to_date_period(base_date):
    """당월 1일 ~ 오늘(exclusive) MTD 기간."""
    start = base_date.replace(day=1)
    end   = base_date
    return {'Start': start.strftime('%Y-%m-%d'), 'End': end.strftime('%Y-%m-%d')}


# ─────────────────────────────────────────
# 태그 파싱 헬퍼
# ─────────────────────────────────────────

def parse_tag_key(raw_key):
    """
    Cost Explorer TAG GroupBy 응답에서 실제 태그값 추출.

    AWS 응답 포맷: "aws:createdBy$<tag_value>"
    미태깅:       "aws:createdBy$"  → "(untagged)" 반환
    """
    if '$' in raw_key:
        value = raw_key.split('$', 1)[1]
        return value if value else '(untagged)'
    return raw_key


def shorten_creator(creator):
    """
    IAM 유저 ARN을 짧게 표시.
    예: "arn:aws:iam::123456789012:user/john" → "user/john"
         "assumed-role/AWSReservedSSO_xxx/user@example.com" → 마지막 3분할 그대로
    """
    if creator.startswith('arn:aws:'):
        # ARN 마지막 세그먼트 (resource 부분)
        return creator.split(':')[-1]
    return creator


# ─────────────────────────────────────────
# 탐색 함수
# ─────────────────────────────────────────

def explore_costs_by_creator(ce_client, period, label):
    """
    TAG(aws:createdBy) 단일 GroupBy → IAM 유저별 총 비용.

    GroupBy: [TAG: aws:createdBy]
    Metrics: UnblendedCost
    """
    printer.print_header(f"[{label}] IAM 유저별 총 비용 (aws:createdBy 단일 GroupBy)", "get_cost_and_usage")
    print(f"  TimePeriod: {period['Start']} ~ {period['End']}")
    print(f"  GroupBy: TAG(aws:createdBy) 단일")

    response = ce_client.get_cost_and_usage(
        TimePeriod=period,
        Granularity='MONTHLY',
        Metrics=['UnblendedCost'],
        GroupBy=[
            {'Type': 'TAG', 'Key': 'aws:createdBy'},
        ],
    )

    printer.print_section("원본 응답 구조 (ResultsByTime[0])")
    printer.print_response(response['ResultsByTime'][0])

    # 파싱: {creator: float}
    groups = response['ResultsByTime'][0].get('Groups', [])
    costs = {}
    for group in groups:
        raw_key = group['Keys'][0]          # "aws:createdBy$<value>"
        creator = parse_tag_key(raw_key)
        amount  = float(group['Metrics']['UnblendedCost']['Amount'])
        costs[creator] = costs.get(creator, 0.0) + amount

    sorted_costs = sorted(costs.items(), key=lambda x: x[1], reverse=True)

    printer.print_section("파싱 결과 — IAM 유저별 비용 (내림차순)")
    printer.print_response({'creators': [
        {'Creator': shorten_creator(c), 'FullARN': c, 'Cost': f'${v:,.4f}'}
        for c, v in sorted_costs
    ]})

    total = sum(costs.values())
    printer.print_key_info({
        '기간': f"{period['Start']} ~ {period['End']}",
        '총 비용': f'${total:,.4f}',
        'IAM 유저 수': len(costs),
        'Top 5': [f"{shorten_creator(c)}: ${v:,.4f}" for c, v in sorted_costs[:5]],
    })

    printer.print_parsing_tips([
        "Keys[0] = 'aws:createdBy$<tag_value>' 형태 — $ 앞은 태그 키 이름",
        "미태깅 리소스: 'aws:createdBy$' (value 비어있음) → '(untagged)' 처리",
        "tag_value는 IAM ARN: arn:aws:iam::123:user/john 또는 assumed-role ARN",
        "aws:createdBy 태그가 Billing 콘솔에서 활성화되지 않으면 Groups=[] 반환",
    ])

    return costs


def explore_costs_by_service_for_creator(ce_client, period, creator_arn, label):
    """
    특정 IAM 유저 필터 + SERVICE GroupBy → 해당 유저의 서비스별 비용.

    Filter:  TAG(aws:createdBy) = creator_arn
    GroupBy: [DIMENSION: SERVICE]
    Metrics: UnblendedCost
    """
    printer.print_header(
        f"[{label}] 특정 유저 필터 → 서비스별 비용",
        "get_cost_and_usage"
    )
    print(f"  TimePeriod: {period['Start']} ~ {period['End']}")
    print(f"  Filter: aws:createdBy = {creator_arn!r}")
    print(f"  GroupBy: DIMENSION(SERVICE)")

    response = ce_client.get_cost_and_usage(
        TimePeriod=period,
        Granularity='MONTHLY',
        Metrics=['UnblendedCost'],
        Filter={
            'Tags': {
                'Key':    'aws:createdBy',
                'Values': [creator_arn],
            }
        },
        GroupBy=[
            {'Type': 'DIMENSION', 'Key': 'SERVICE'},
        ],
    )

    printer.print_section("원본 응답 구조 (ResultsByTime[0])")
    printer.print_response(response['ResultsByTime'][0])

    groups = response['ResultsByTime'][0].get('Groups', [])
    costs = {}
    for group in groups:
        service = group['Keys'][0]
        amount  = float(group['Metrics']['UnblendedCost']['Amount'])
        costs[service] = amount

    sorted_costs = sorted(costs.items(), key=lambda x: x[1], reverse=True)
    total = sum(costs.values())

    printer.print_section(f"파싱 결과 — {shorten_creator(creator_arn)} 서비스별 비용")
    printer.print_response({'services': [
        {'Service': s, 'Cost': f'${v:,.4f}'} for s, v in sorted_costs
    ]})

    printer.print_key_info({
        '조회 유저': shorten_creator(creator_arn),
        '총 비용': f'${total:,.4f}',
        '서비스 수': len(costs),
    })

    printer.print_parsing_tips([
        "Filter.Tags.Values 에 태그 값 전체(ARN 포함)를 넣어야 함",
        "필터 후 GroupBy는 1개만 사용 가능 → SERVICE or INSTANCE_TYPE 등",
        "여러 유저 동시 필터: Values 리스트에 여러 ARN 추가 (OR 조건)",
    ])

    return costs


# ─────────────────────────────────────────
# 메인
# ─────────────────────────────────────────

def main():
    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + "  API 7: Cost Explorer - aws:createdBy 태그 기반 IAM 유저별 비용 집계".center(78) + "║")
    print("╚" + "=" * 78 + "╝")

    setup_environment()

    profile = os.environ.get('AWS_PROFILE', 'default')
    print(f"🔄 AWS Cost Explorer API 호출 중... (profile={profile})\n")

    print("  ⚠️  사전 조건 체크:")
    print("      AWS Billing 콘솔 > Cost allocation tags > 'aws:createdBy' 활성화 필요")
    print("      미활성화 시 Groups=[] 반환 (비용 $0으로 보임)\n")

    session   = boto3.Session(profile_name=profile)
    ce_client = session.client('ce', region_name='us-east-1')

    today      = date.today()
    period_mtd = build_month_to_date_period(today)

    print(f"  MTD 기간: {period_mtd['Start']} ~ {period_mtd['End']}\n")

    try:
        # 1. IAM 유저별 월간 총 비용 (TAG 단일 GroupBy)
        costs_by_creator = explore_costs_by_creator(ce_client, period_mtd, "MTD")

        # 2. 상위 유저 1명에 대해 서비스별 상세 조회 (필터 방식)
        if costs_by_creator:
            top_creator = max(costs_by_creator, key=costs_by_creator.get)
            if top_creator != '(untagged)':
                explore_costs_by_service_for_creator(
                    ce_client, period_mtd, top_creator, "MTD Top유저"
                )
            else:
                print("\n⚠️  상위 유저가 (untagged)이므로 필터 상세 조회 스킵")

    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return

    print("\n" + "=" * 80)
    print("✅ API 7 탐색 완료 — aws:createdBy 태그 기반 IAM 유저별 비용 집계")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
