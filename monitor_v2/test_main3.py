"""
monitor_v2/test_main3.py

Main 3 (비용 변화 AI 분석) 로컬 테스트.

실행:
    uv run python -m monitor_v2.test_main3
    uv run python -m monitor_v2.test_main3 --date 2026-04-07
"""

import argparse
from datetime import date, datetime
from print_test.utils.environment import setup_environment
setup_environment()

def main():
    parser = argparse.ArgumentParser(description='Main 3 로컬 테스트')
    parser.add_argument(
        '--date', default=None,
        help='d1_date (YYYY-MM-DD). 기본값: today - 1',
    )
    args = parser.parse_args()

    if args.date:
        d1_date = datetime.strptime(args.date, '%Y-%m-%d').date()
    else:
        from datetime import timedelta
        d1_date = date.today() - timedelta(days=1)

    print(f"[test_main3] d1_date={d1_date}")

    from monitor_v2.cost.report_analysis import send_main3_report
    send_main3_report(d1_date)

    print("[test_main3] 완료")


if __name__ == '__main__':
    main()
