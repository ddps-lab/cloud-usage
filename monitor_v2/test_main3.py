"""
monitor_v2/test_main3.py

Main 3 (비용 변화 AI 분석) 로컬 테스트.

실행:
    uv run python -m monitor_v2.test_main3
    uv run python -m monitor_v2.test_main3 --date 2026-04-07
    uv run python -m monitor_v2.test_main3 --date 2026-11-01 --dry-run   # Slack 에 안 보내고 화면에만
"""

import argparse
import json
from datetime import date, datetime
from print_test.utils.environment import setup_environment
setup_environment()

def main():
    parser = argparse.ArgumentParser(description='Main 3 로컬 테스트')
    parser.add_argument(
        '--date', default=None,
        help='d1_date (YYYY-MM-DD). 기본값: today - 1',
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Slack 에 보내지 않고 데이터 소스 / 요약 수치 / 블록만 출력',
    )
    args = parser.parse_args()

    if args.date:
        d1_date = datetime.strptime(args.date, '%Y-%m-%d').date()
    else:
        from datetime import timedelta
        d1_date = date.today() - timedelta(days=1)

    print(f"[test_main3] d1_date={d1_date}")

    if args.dry_run:
        from monitor_v2.cost.analysis import collect_all
        from monitor_v2.cost.report_analysis import _build_main3
        analysis = collect_all(d1_date)
        print(f"[test_main3] data_source={analysis['data_source']}"
              f"  fallback_reason={analysis['fallback_reason']}")
        print(f"[test_main3] d1_total=${analysis['d1_total']:,.2f}"
              f"  d2_total=${analysis['d2_total']:,.2f}"
              f"  mtd_total=${analysis['mtd_total']:,.2f}"
              f"  forecast_total=${analysis['forecast_total']:,.2f}")
        blocks = [b.to_dict() if hasattr(b, 'to_dict') else b
                  for b in _build_main3(analysis)]
        print(json.dumps(blocks, ensure_ascii=False, indent=2))
        if analysis.get('llm_error'):
            print(f"[test_main3] llm_error={analysis['llm_error']!r}")
        print("[test_main3] dry-run 완료 (Slack 미발송)")
        return

    from monitor_v2.cost.report_analysis import send_main3_report
    send_main3_report(d1_date)

    print("[test_main3] 완료")


if __name__ == '__main__':
    main()
