"""
monitor_v2/test_cost.py

cost/data.py 단독 테스트 — Slack 발송 없이 수집 결과를 print.

lambda_handler.py 와 동일하게 collect_all()을 호출하고,
반환된 8개 키를 섹션별로 출력한다.

사용법:
    uv run python -m monitor_v2.test_cost
    또는
    python -m monitor_v2.test_cost
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
from pprint import pprint

# 프로젝트 루트(cloud-usage/)를 경로에 추가 → monitor_v2 패키지 import 가능
sys.path.insert(0, str(Path(__file__).parent.parent))

from print_test.utils.environment import setup_environment
from monitor_v2.cost.data import collect_all as collect_cost_data

SEP  = "─" * 70
SEP2 = "=" * 70
KST  = timezone(timedelta(hours=9))


def _fmt_service_cost(costs: dict, top_n: int = None) -> None:
    """서비스별 비용 dict를 내림차순으로 출력."""
    sorted_items = sorted(costs.items(), key=lambda x: x[1], reverse=True)
    if top_n:
        sorted_items = sorted_items[:top_n]
    for service, cost in sorted_items:
        if cost > 0:
            print(f"    {service:<45}  ${cost:>10,.4f}")


def _fmt_by_creator(by_creator: dict) -> None:
    """서비스 → 생성자 구조를 creator별 총합 내림차순으로 출력."""
    creator_totals = {}
    creator_services = {}
    for service, creators in by_creator.items():
        for creator, cost in creators.items():
            creator_totals[creator] = creator_totals.get(creator, 0.0) + cost
            creator_services.setdefault(creator, {})
            creator_services[creator][service] = creator_services[creator].get(service, 0.0) + cost

    for creator, total in sorted(creator_totals.items(), key=lambda x: x[1], reverse=True):
        if total <= 0:
            continue
        short = creator.split(':')[-1] if creator.startswith('IAMUser') else creator
        print(f"  👤 {short:<45}  합계: ${total:,.4f}")
        for svc, cost in sorted(creator_services[creator].items(), key=lambda x: x[1], reverse=True):
            if cost > 0:
                print(f"       ├─ {svc:<40}  ${cost:,.4f}")
        print()


def _fmt_by_creator_mtd(by_creator_mtd: dict, forecast: float) -> None:
    """MTD creator별 총합 내림차순 + 비례 예측 출력."""
    creator_totals = {}
    creator_services = {}
    for service, creators in by_creator_mtd.items():
        for creator, cost in creators.items():
            creator_totals[creator] = creator_totals.get(creator, 0.0) + cost
            creator_services.setdefault(creator, {})
            creator_services[creator][service] = creator_services[creator].get(service, 0.0) + cost

    if not creator_totals:
        print("    (데이터 없음)\n")
        return

    total_mtd = sum(creator_totals.values())
    for creator, total in sorted(creator_totals.items(), key=lambda x: x[1], reverse=True):
        if total <= 0:
            continue
        short = creator.split(':')[-1] if creator.startswith('IAMUser') else creator
        if forecast > 0 and total_mtd > 0:
            fc  = forecast * (total / total_mtd)
            prj = total + fc
            fstr = f"  → 예상 ${prj:,.4f} (+${fc:,.4f} 추정)"
        else:
            fstr = ""
        print(f"  👤 {short:<45}  MTD: ${total:,.4f}{fstr}")
        for svc, cost in sorted(creator_services[creator].items(), key=lambda x: x[1], reverse=True):
            if cost > 0:
                print(f"       ├─ {svc:<40}  ${cost:,.4f}")
        print()
    if forecast <= 0:
        print("  ※ 잔여 예측 없음 (CE 예측 API 미응답)\n")


def _fmt_by_region(by_region: dict) -> None:
    """서비스 → 리전 구조를 서비스 총합 내림차순으로 출력."""
    if not by_region:
        print("    (데이터 없음)\n")
        return
    for service, regions in sorted(by_region.items(), key=lambda x: sum(x[1].values()), reverse=True):
        total = sum(regions.values())
        if total <= 0:
            continue
        print(f"  📌 {service:<45}  ${total:,.4f}")
        for region, cost in sorted(regions.items(), key=lambda x: x[1], reverse=True):
            if cost > 0:
                print(f"       ├─ {region:<30}  ${cost:,.4f}")
        print()


def _fmt_by_region_mtd(by_region_mtd: dict, forecast: float) -> None:
    """MTD 서비스별 리전 구조 + 서비스 MTD 비율 비례 예측 출력."""
    if not by_region_mtd:
        print("    (데이터 없음)\n")
        return
    total_mtd = sum(
        sum(r.values())
        for r in by_region_mtd.values()
        if sum(r.values()) > 0
    )
    for service, regions in sorted(by_region_mtd.items(), key=lambda x: sum(x[1].values()), reverse=True):
        total = sum(regions.values())
        if total <= 0:
            continue
        if forecast > 0 and total_mtd > 0:
            svc_fc    = forecast * (total / total_mtd)
            projected = total + svc_fc
            fstr = f"  → 예상 ${projected:,.4f} (+${svc_fc:,.4f} 추정)"
        else:
            fstr = ""
        print(f"  📌 {service:<45}  MTD: ${total:,.4f}{fstr}")
        for region, cost in sorted(regions.items(), key=lambda x: x[1], reverse=True):
            if cost > 0:
                print(f"       ├─ {region:<30}  ${cost:,.4f}")
        print()
    if forecast <= 0:
        print("  ※ 잔여 예측 없음 (CE 예측 API 미응답)\n")


def main():
    print("\n" + SEP2)
    print("  monitor_v2 / cost/data.py — 단독 테스트")
    print(SEP2)

    setup_environment()

    # CE는 UTC 자정 기준으로 하루를 끊으므로 UTC date를 today로 사용.
    # KST date를 쓰면 자정~09:00 KST 구간에서 D-1이 하루 어긋날 수 있음.
    today_kst = datetime.now(timezone.utc).date() #- timedelta(days=1)
    print(f"\n  기준 날짜 (UTC date, CE 컷오프 기준): {today_kst}")
    print(f"  리포트 대상 (D-1):                   {today_kst - timedelta(days=1)}")
    print(f"  현재 KST 시각:                        {datetime.now(KST).strftime('%Y-%m-%d %H:%M')}")
    print()

    print("▶ collect_all() 호출 중 (API 10회)...")
    cost_data = collect_cost_data(today_kst)
    print("  완료\n")

    d1_date  = cost_data['d1_date']
    daily_d1 = cost_data['daily_d1']
    daily_d2 = cost_data['daily_d2']
    daily_lm = cost_data['daily_lm']

    # ── 1. 날짜 정보 ─────────────────────────────────────────────────────────
    print(SEP)
    print(f"[1] 날짜 정보")
    print(SEP)
    print(f"  d1_date (리포트 대상일):    {d1_date}")
    print(f"  daily_d1 기간:              {d1_date}")
    print(f"  daily_d2 기간:              {today_kst - timedelta(days=2)}")
    print()

    # ── 2. D-1 서비스별 비용 ─────────────────────────────────────────────────
    total_d1 = sum(daily_d1.values())
    print(SEP)
    print(f"[2] D-1 서비스별 비용  (총 ${total_d1:,.4f})")
    print(SEP)
    _fmt_service_cost(daily_d1)
    print()

    # ── 3. D-2 서비스별 비용 ─────────────────────────────────────────────────
    total_d2 = sum(daily_d2.values())
    print(SEP)
    print(f"[3] D-2 서비스별 비용  (총 ${total_d2:,.4f})")
    print(SEP)
    _fmt_service_cost(daily_d2)
    print()

    # ── 4. 전월 동일일 서비스별 비용 ─────────────────────────────────────────
    total_lm = sum(daily_lm.values())
    print(SEP)
    print(f"[4] 전월 동일일 서비스별 비용  (총 ${total_lm:,.4f})")
    print(SEP)
    _fmt_service_cost(daily_lm)
    print()

    # ── 5. MTD ───────────────────────────────────────────────────────────────
    mtd_this = cost_data['mtd_this']
    mtd_prev = cost_data['mtd_prev']
    forecast = cost_data.get('forecast', 0.0)
    print(SEP)
    print(f"[5] MTD (Month-To-Date) 누계 + 예측")
    print(SEP)
    print(f"  이번달 MTD:   ${mtd_this:>12,.4f}")
    print(f"  전월   MTD:   ${mtd_prev:>12,.4f}")
    if mtd_prev:
        diff = mtd_this - mtd_prev
        pct  = diff / mtd_prev * 100
        print(f"  전월 대비:    {'+' if diff >= 0 else ''}${diff:,.4f}  ({'+' if pct >= 0 else ''}{pct:.1f}%)")
    if forecast > 0:
        projected = mtd_this + forecast
        print(f"  잔여 예측:    +${forecast:>11,.4f}")
        print(f"  이달 예상:    ${projected:>12,.4f}")
    else:
        print(f"  잔여 예측:    (데이터 없음 — CE 예측 API 미응답)")
    print()

    # ── 6. IAM User별 (by_creator) D-1 + MTD ────────────────────────────────
    forecast       = cost_data.get('forecast', 0.0)
    by_creator_mtd = cost_data.get('by_creator_mtd', {})
    by_region_mtd  = cost_data.get('by_region_mtd', {})

    print(SEP)
    print(f"[6] aws:createdBy 태그별 비용  (Thread 2 원본)")
    print(SEP)
    print("  ※ '(태그 없음 / 공용)' = aws:createdBy 미태깅 리소스\n")
    print(f"  ▸ D-1 당일")
    _fmt_by_creator(cost_data['by_creator'])
    print(f"  ▸ 당월 누계 (MTD)")
    _fmt_by_creator_mtd(by_creator_mtd, forecast)

    # ── 7. 서비스 + 리전별 (by_region) D-1 + MTD ────────────────────────────
    print(SEP)
    print(f"[7] 서비스 + 리전별 비용  (Thread 3 원본, EC2 포함 전체)")
    print(SEP)
    print(f"  ▸ D-1 당일")
    _fmt_by_region(cost_data['by_region'])
    print(f"  ▸ 당월 누계 (MTD) + 잔여 예측 (서비스 MTD 비율 비례 추정)")
    _fmt_by_region_mtd(by_region_mtd, forecast)

    # ── 8. 원본 dict 요약 ────────────────────────────────────────────────────
    print(SEP)
    print(f"[8] collect_all() 반환 dict 키 목록")
    print(SEP)
    for key, val in cost_data.items():
        if isinstance(val, dict):
            print(f"  '{key}': dict  ({len(val)}개 항목)")
        elif isinstance(val, float):
            extra = '  ← 예측 불가' if key == 'forecast' and val == 0.0 else ''
            print(f"  '{key}': float = ${val:,.4f}{extra}")
        else:
            print(f"  '{key}': {type(val).__name__} = {val}")
    print()

    print(SEP2)
    print("  완료 — Slack 발송 없음")
    print(SEP2 + "\n")


if __name__ == "__main__":
    main()
