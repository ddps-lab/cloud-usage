"""
API 6: Cost Explorer - get_cost_and_usage

목적: 실제 AWS API 호출로 응답 구조 및 비용 데이터 확인

확인 항목:
    1. SERVICE 기준 — 전체 서비스 비용 (오늘 / 전일 / 전월 동일일)
    2. EC2 필터 + INSTANCE_TYPE 기준 — EC2 instance_type별 비용 및 실행시간

사용법:
    python -m print_test.cost_explorer.get_cost_and_usage
    또는
    uv run python -m print_test.cost_explorer.get_cost_and_usage
"""

import os
import sys
from calendar import monthrange
from datetime import date, timedelta
from pathlib import Path
from pprint import pprint

import boto3

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from print_test.utils.environment import setup_environment
from print_test.utils.printer import StructuredPrinter

printer = StructuredPrinter()


# ─────────────────────────────────────────
# 날짜 계산
# ─────────────────────────────────────────

def build_period(base_date, days_ago):
    """Cost Explorer TimePeriod dict. End는 exclusive이므로 +1일."""
    start = base_date - timedelta(days=days_ago)
    end   = start + timedelta(days=1)
    return {'Start': start.strftime('%Y-%m-%d'), 'End': end.strftime('%Y-%m-%d')}


def build_last_month_period(base_date, days_ago):
    """전월 동일일 TimePeriod. 말일 클램핑 포함."""
    target = base_date - timedelta(days=days_ago)
    month  = target.month - 1 if target.month > 1 else 12
    year   = target.year      if target.month > 1 else target.year - 1
    day    = min(target.day, monthrange(year, month)[1])
    start  = date(year, month, day)
    end    = start + timedelta(days=1)
    return {'Start': start.strftime('%Y-%m-%d'), 'End': end.strftime('%Y-%m-%d')}


def build_month_period(year, month):
    """특정 월 전체 TimePeriod. End는 다음달 1일 (exclusive)."""
    start = date(year, month, 1)
    next_month = month + 1 if month < 12 else 1
    next_year  = year if month < 12 else year + 1
    end = date(next_year, next_month, 1)
    return {'Start': start.strftime('%Y-%m-%d'), 'End': end.strftime('%Y-%m-%d')}


# ─────────────────────────────────────────
# 응답 탐색 함수
# ─────────────────────────────────────────

def explore_service_costs(ce_client, period, label):
    """
    SERVICE 기준 비용 조회 및 응답 구조 출력.

    GroupBy: [SERVICE] 단일
    Metrics: UnblendedCost
    """
    printer.print_header(f"[{label}] 전체 서비스 비용", "get_cost_and_usage")
    print(f"  TimePeriod: {period['Start']} ~ {period['End']}")

    response = ce_client.get_cost_and_usage(
        TimePeriod=period,
        Granularity='DAILY',
        Metrics=['UnblendedCost'],
        GroupBy=[{'Type': 'DIMENSION', 'Key': 'SERVICE'}],
    )

    printer.print_section("원본 응답 구조 (ResultsByTime[0])")
    printer.print_response(response['ResultsByTime'][0])

    # 파싱: {service: float}
    groups = response['ResultsByTime'][0].get('Groups', [])
    costs = {}
    for group in groups:
        service = group['Keys'][0]
        amount  = float(group['Metrics']['UnblendedCost']['Amount'])
        costs[service] = amount

    # 비용 내림차순 정렬
    sorted_costs = sorted(costs.items(), key=lambda x: x[1], reverse=True)

    printer.print_section("파싱 결과 — 서비스별 비용 (내림차순)")
    printer.print_response({'services': [
        {'Service': s, 'Cost': f'${c:,.4f}'} for s, c in sorted_costs
    ]})

    total = sum(costs.values())
    estimated = response['ResultsByTime'][0].get('Estimated', False)
    printer.print_key_info({
        '기간': f"{period['Start']} ~ {period['End']}",
        '총 비용': f'${total:,.4f}',
        '서비스 수': len(costs),
        'Estimated': estimated,
        'Top 5': [f"{s}: ${c:,.4f}" for s, c in sorted_costs[:5]],
    })

    printer.print_parsing_tips([
        "Keys[0] = 서비스 이름 (GroupBy=SERVICE 단일이므로 Keys는 1개 원소)",
        "Amount는 문자열 반환 → float() 변환 필수",
        "Estimated=True: 해당 날짜 데이터가 아직 미확정",
        "Groups가 빈 리스트 = 해당 기간 비용 $0",
    ])

    return costs


def explore_monthly_top_service(ce_client, period, label):
    """
    월 단위 누적 비용 기준 서비스별 + IAM 유저별 비용 조회.

    Granularity: MONTHLY (기간 전체를 1개 결과로 반환)
    GroupBy: [SERVICE, TAG(aws:createdBy)] — 서비스별 생성자 비용 분리
    Metrics: UnblendedCost

    주의: GroupBy 최대 2개 제한 → USAGE_TYPE과 동시 사용 불가
    """
    printer.print_header(f"[{label}] 월 누적 서비스별 + 생성자별 비용", "get_cost_and_usage")
    print(f"  TimePeriod: {period['Start']} ~ {period['End']} (End는 exclusive)")
    print(f"  GroupBy: SERVICE + TAG(aws:createdBy)")

    response = ce_client.get_cost_and_usage(
        TimePeriod=period,
        Granularity='MONTHLY',
        Metrics=['UnblendedCost'],
        GroupBy=[
            {'Type': 'DIMENSION', 'Key': 'SERVICE'},
            {'Type': 'TAG',       'Key': 'aws:createdBy'},
        ],
    )

    printer.print_section("원본 응답 구조 (ResultsByTime[0])")
    printer.print_response(response['ResultsByTime'][0])

    # 파싱: {service: float} 및 {service: {creator: float}}
    groups = response['ResultsByTime'][0].get('Groups', [])
    costs = {}
    by_creator = {}  # {service: {creator: cost}}

    for group in groups:
        service  = group['Keys'][0]
        raw_tag  = group['Keys'][1]   # "aws:createdBy$<tag_value>"
        creator  = raw_tag.split('$', 1)[1] if '$' in raw_tag else raw_tag
        creator  = creator if creator else '(untagged)'
        amount   = float(group['Metrics']['UnblendedCost']['Amount'])

        costs[service] = costs.get(service, 0.0) + amount
        by_creator.setdefault(service, {})
        by_creator[service][creator] = by_creator[service].get(creator, 0.0) + amount

    sorted_costs = sorted(costs.items(), key=lambda x: x[1], reverse=True)

    # 표시: 서비스별 합계 + 상위 생성자 인라인
    display_rows = []
    for s, c in sorted_costs:
        display_rows.append({'Service': s, 'Cost': f'${c:,.4f}'})
        top_creators = sorted(by_creator.get(s, {}).items(), key=lambda x: x[1], reverse=True)[:3]
        for creator, ccost in top_creators:
            short = creator.split(':')[-1] if creator.startswith('IAMUser') else creator
            display_rows.append({'Service': f'  └─ {short}', 'Cost': f'${ccost:,.4f}'})

    printer.print_section("파싱 결과 — 서비스별 월 누적 비용 (생성자 Top3 인라인)")
    printer.print_response({'services': display_rows})

    total = sum(costs.values())
    estimated = response['ResultsByTime'][0].get('Estimated', False)
    top_service, top_cost = sorted_costs[0] if sorted_costs else ('없음', 0.0)

    printer.print_key_info({
        '기간': f"{period['Start']} ~ {period['End']}",
        '총 비용': f'${total:,.4f}',
        '서비스 수': len(costs),
        'Estimated': estimated,
        '최다 비용 서비스': top_service,
        '최다 비용': f'${top_cost:,.4f}',
        '비율': f'{top_cost / total * 100:.1f}%' if total > 0 else 'N/A',
        'Top 5 서비스': [f"{s}: ${c:,.4f}" for s, c in sorted_costs[:5]],
    })

    printer.print_parsing_tips([
        "GroupBy=[SERVICE, TAG(aws:createdBy)] → Keys[0]=서비스, Keys[1]='aws:createdBy$<ARN>'",
        "creator 추출: Keys[1].split('$', 1)[1]  ($ 앞은 태그 키 이름)",
        "미태깅 리소스: Keys[1]='aws:createdBy$' → creator='' → '(untagged)' 처리",
        "GroupBy 최대 2개 제한: USAGE_TYPE과 동시 사용 불가 → 별도 호출 필요",
        "aws:createdBy 미활성화 시 Groups에 creator=(untagged) 단일 행만 반환",
    ])

    return costs, top_service, top_cost


def explore_ec2_by_instance_type(ce_client, period, label):
    """
    EC2 필터 + INSTANCE_TYPE 기준 비용 및 실행시간 조회.

    GroupBy: [INSTANCE_TYPE, REGION] (최대 2개 제약)
    Metrics: UnblendedCost + UsageQuantity (실행시간 단위: Hrs)
    Filter:  SERVICE = 'Amazon EC2'
    """
    printer.print_header(f"[{label}] EC2 — instance_type별 비용 및 실행시간", "get_cost_and_usage")
    print(f"  TimePeriod: {period['Start']} ~ {period['End']}")
    print(f"  Filter: Amazon EC2 only")
    print(f"  GroupBy: INSTANCE_TYPE + REGION")

    response = ce_client.get_cost_and_usage(
        TimePeriod=period,
        Granularity='DAILY',
        Metrics=['UnblendedCost', 'UsageQuantity'],
        Filter={
            'Dimensions': {
                'Key': 'SERVICE',
                'Values': ['Amazon EC2'],
            }
        },
        GroupBy=[
            {'Type': 'DIMENSION', 'Key': 'INSTANCE_TYPE'},
            {'Type': 'DIMENSION', 'Key': 'REGION'},
        ],
    )

    printer.print_section("원본 응답 구조 (ResultsByTime[0])")
    printer.print_response(response['ResultsByTime'][0])

    # 파싱: {instance_type: {cost, hours}} — REGION 무시하고 타입별 합산
    groups = response['ResultsByTime'][0].get('Groups', [])
    by_type = {}
    for group in groups:
        instance_type = group['Keys'][0]
        region        = group['Keys'][1]
        cost  = float(group['Metrics']['UnblendedCost']['Amount'])
        hours = float(group['Metrics']['UsageQuantity']['Amount'])

        if instance_type not in by_type:
            by_type[instance_type] = {'cost': 0.0, 'hours': 0.0, 'regions': []}
        by_type[instance_type]['cost']  += cost
        by_type[instance_type]['hours'] += hours
        if region not in by_type[instance_type]['regions']:
            by_type[instance_type]['regions'].append(region)

    sorted_types = sorted(by_type.items(), key=lambda x: x[1]['cost'], reverse=True)

    printer.print_section("파싱 결과 — instance_type별 집계 (비용 내림차순)")
    printer.print_response({'instance_types': [
        {
            'InstanceType': t,
            'Cost':    f"${d['cost']:,.4f}",
            'Hours':   f"{d['hours']:.1f} Hrs",
            'Regions': d['regions'],
        }
        for t, d in sorted_types
    ]})

    total_ec2 = sum(d['cost'] for d in by_type.values())
    printer.print_key_info({
        '기간': f"{period['Start']} ~ {period['End']}",
        'EC2 총 비용': f'${total_ec2:,.4f}',
        'instance_type 수': len(by_type),
        'Top 5 (비용 기준)': [
            f"{t}: ${d['cost']:,.4f} / {d['hours']:.1f}Hrs"
            for t, d in sorted_types[:5]
        ],
    })

    printer.print_parsing_tips([
        "GroupBy 2개: Keys[0]=INSTANCE_TYPE, Keys[1]=REGION",
        "UsageQuantity 단위는 'Hrs' (BoxUsage 기반 청구 시간)",
        "같은 instance_type이 여러 리전에 걸쳐 나오므로 타입별 합산 필요",
        "GroupBy 최대 2개 제약: INSTANCE_TYPE + LINKED_ACCOUNT 동시 사용 불가",
    ])

    return by_type


# ─────────────────────────────────────────
# 메인
# ─────────────────────────────────────────

def main():
    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + "  API 6: Cost Explorer - get_cost_and_usage".center(78) + "║")
    print("╚" + "=" * 78 + "╝")

    setup_environment()

    profile = os.environ.get('AWS_PROFILE', 'default')
    print(f"🔄 AWS Cost Explorer API 호출 중... (profile={profile})\n")

    # Cost Explorer는 글로벌 단일 엔드포인트 (us-east-1 고정)
    session   = boto3.Session(profile_name=profile)
    ce_client = session.client('ce', region_name='us-east-1')

    today   = date.today()
    days_ago = 1  # D-1 기준 (확정값)

    period_d1 = build_period(today, days_ago)
    period_d2 = build_period(today, days_ago + 1)
    period_lm = build_last_month_period(today, days_ago)

    print(f"  D-1 (리포트 대상):  {period_d1['Start']}")
    print(f"  D-2 (전일 비교):    {period_d2['Start']}")
    print(f"  전월 동일일:        {period_lm['Start']}\n")

    # 26년 3월 전체 누적 기간
    period_mar2026 = build_month_period(2026, 3)
    print(f"  26년 03월 누적:      {period_mar2026['Start']} ~ {period_mar2026['End']}\n")

    try:
        # 1. 전체 서비스 — 3개 기간
        costs_d1 = explore_service_costs(ce_client, period_d1, "D-1 (오늘)")
        costs_d2 = explore_service_costs(ce_client, period_d2, "D-2 (전일)")
        costs_lm = explore_service_costs(ce_client, period_lm, "전월 동일일")

        # 2. EC2 instance_type — D-1만 (구조 확인용)
        explore_ec2_by_instance_type(ce_client, period_d1, "D-1 EC2")

        # 3. 26년 3월 월 누적 최다 비용 서비스
        explore_monthly_top_service(ce_client, period_mar2026, "26년 03월 누적")

        # 4. 전체 서비스 3기간 비교 요약
        printer.print_header("비교 요약 — D-1 vs D-2 vs 전월 동일일", "종합")

        all_services = set(costs_d1) | set(costs_d2) | set(costs_lm)
        sorted_by_today = sorted(all_services, key=lambda s: costs_d1.get(s, 0.0), reverse=True)

        rows = []
        for service in sorted_by_today[:10]:
            c1 = costs_d1.get(service, 0.0)
            c2 = costs_d2.get(service, 0.0)
            clm = costs_lm.get(service, 0.0)
            d_yday = f"{'+' if c1-c2 >= 0 else ''}{c1-c2:+.2f}" if c2 else "N/A"
            d_lm   = f"{'+' if c1-clm >= 0 else ''}{c1-clm:+.2f}" if clm else "N/A"
            rows.append({
                'Service': service[:40],
                'D-1':     f"${c1:,.2f}",
                'vs D-2':  d_yday,
                'vs 전월': d_lm,
            })

        printer.print_response({'top10_comparison': rows}, max_items=10)

    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return

    print("\n" + "=" * 80)
    print("✅ API 6 탐색 완료!")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
