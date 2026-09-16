"""
monitor_v2/test_main4.py

Main 4 (hyperun 으로 낸 job) 로컬 테스트. **진짜 Slack 에 보낸다.**

실행:
    uv run python -m monitor_v2.test_main4
    uv run python -m monitor_v2.test_main4 --days 7
    uv run python -m monitor_v2.test_main4 --dry-run     # 안 보내고 화면에만
    uv run python -m monitor_v2.test_main4 --sample      # gateway 없이 예시 숫자로

무엇을 확인하는가:
    `report_test.py` 가 블록의 모양을 16개로 이미 지키고 있다. 이 script 가 지키는
    것은 그 다음 한 칸 — gateway 가 실제로 답하는지, 그 답이 블록으로 만들어지는지,
    Slack 이 그 블록을 받는지. 셋 다 단위 test 로는 확인할 수 없다.

    `test_main3.py` 와 같은 모양이고 CI 는 안 돈다.

환경변수 (.env 또는 셸):
    HYPERUN_API_BASE    gateway 주소. 예: https://api.hyperun.example.com
    HYPERUN_API_TOKEN   **operator 토큰**. /v1/usage 는 operator 전용이라 일반
                        토큰이면 403 이 온다.
    SLACK_BOT_TOKEN     Main 1 이 쓰는 것과 같다.
    SLACK_CHANNEL_ID    같다.

    ★ --sample 을 쓰면 앞의 둘이 없어도 된다. 화면 모양만 보고 싶을 때를 위한 것이고,
    그때는 숫자가 진짜가 아니라는 것을 출력이 분명히 말한다.
"""

import argparse
import json
from datetime import date, datetime, timedelta, timezone

from print_test.utils.environment import setup_environment
setup_environment()


def sample_usage(days: int) -> dict:
    """gateway 없이 화면 모양만 볼 때 쓰는 숫자.

    2026-09-15 의 실제 job 에서 뽑은 모양이다. 완전히 지어낸 값이면 "이게 실제로
    이렇게 보이나" 를 판단할 수 없어서, 자릿수와 이름을 진짜에 맞춰 뒀다.
    """
    today = datetime.now(timezone.utc).date()
    series = []
    for offset in range(days - 1, -1, -1):
        day = today - timedelta(days=offset)
        if offset == 1:                      # 어제
            series.append({'date': str(day), 'jobs': 3, 'gpu_hours': 29.2,
                           'estimate_usd': 186.11, 'unpriced_jobs': 0})
        elif offset == 2:
            series.append({'date': str(day), 'jobs': 1, 'gpu_hours': 4.0,
                           'estimate_usd': 25.44, 'unpriced_jobs': 0})
        else:
            series.append({'date': str(day), 'jobs': 0, 'gpu_hours': 0.0,
                           'estimate_usd': 0.0, 'unpriced_jobs': 0})
    return {
        'days': series,
        'teams': [{'name': 'ddps', 'jobs': 12, 'gpu_hours': 140.0, 'estimate_usd': 890.4,
                   'day_jobs': 3, 'day_gpu_hours': 29.2, 'day_estimate_usd': 186.11}],
        'users': [
            {'name': 'jglee', 'jobs': 9, 'gpu_hours': 120.0, 'estimate_usd': 763.2,
             'day_jobs': 2, 'day_gpu_hours': 25.8, 'day_estimate_usd': 164.02},
            {'name': 'max322318', 'jobs': 3, 'gpu_hours': 20.0, 'estimate_usd': 127.2,
             'day_jobs': 1, 'day_gpu_hours': 3.4, 'day_estimate_usd': 22.09},
        ],
        'vendors': [
            {'name': 'runpod', 'jobs': 10, 'gpu_hours': 130.0, 'estimate_usd': 826.8,
             'day_jobs': 2, 'day_gpu_hours': 25.8, 'day_estimate_usd': 164.02},
            {'name': 'aws', 'jobs': 2, 'gpu_hours': 10.0, 'estimate_usd': 63.6,
             'day_jobs': 1, 'day_gpu_hours': 3.4, 'day_estimate_usd': 22.09},
        ],
        'mtd_estimate_usd': 1204.88, 'mtd_jobs': 12, 'mtd_gpu_hours': 190.5,
        'note': 'estimate',
    }


def show(blocks) -> None:
    """블록을 터미널에서 읽을 수 있게 편다. Slack 이 그리는 모양과 순서가 같다."""
    for b in blocks:
        d = b.to_dict() if hasattr(b, 'to_dict') else b
        kind = d.get('type')
        if kind == 'header':
            print('=' * 70)
            print(' ' + d['text']['text'])
            print('=' * 70)
        elif kind == 'divider':
            print('-' * 70)
        elif kind == 'context':
            print('  ' + ' '.join(e.get('text', '') for e in d.get('elements', [])))
        elif 'fields' in d:
            for f in d['fields']:
                print('   ' + f['text'].replace('\n', '  '))
        elif kind == 'markdown':
            print(d.get('text', ''))
        elif 'text' in d:
            print(' ' + d['text']['text'].replace('\n', '\n  '))


def main() -> int:
    parser = argparse.ArgumentParser(description='Main 4 로컬 테스트')
    parser.add_argument('--days', type=int, default=None,
                        help='며칠치를 받을지. 기본값은 HYPERUN_USAGE_DAYS 또는 30')
    parser.add_argument('--date', default=None,
                        help='보고서 기준일 (YYYY-MM-DD). 본문의 "어제" 는 그 하루 전이다')
    parser.add_argument('--dry-run', action='store_true',
                        help='Slack 에 보내지 않고 만들어진 블록만 보여준다')
    parser.add_argument('--sample', action='store_true',
                        help='gateway 를 부르지 않고 예시 숫자로 모양만 본다')
    args = parser.parse_args()

    report_date = (datetime.strptime(args.date, '%Y-%m-%d').date() if args.date
                   else datetime.now(timezone.utc).date())
    print(f"[test_main4] 기준일={report_date} (본문의 '어제' 는 "
          f"{report_date - timedelta(days=1)})")

    if args.sample:
        usage = sample_usage(args.days or 30)
        print('[test_main4] ★ --sample 이라 숫자가 진짜가 아니다. 모양만 보는 중')
    else:
        from monitor_v2.hyperun.data import collect, UsageUnavailable, API_BASE, API_TOKEN
        print(f"[test_main4] gateway  {'설정됨' if API_BASE else '없음'}")
        print(f"[test_main4] token    {'있음' if API_TOKEN else '없음'} "
              f"({len(API_TOKEN)}자, operator 여야 한다)")
        try:
            usage = collect(args.days)
        except UsageUnavailable as exc:
            print(f"[test_main4] 사용량을 못 받았다: {exc}")
            print('[test_main4] HYPERUN_API_BASE 와 HYPERUN_API_TOKEN 을 .env 에 넣거나, '
                  '--sample 로 모양만 보십시오')
            return 2
        print(f"[test_main4] 받은 날짜 {len(usage.get('days') or [])}일, "
              f"team {len(usage.get('teams') or [])} / "
              f"user {len(usage.get('users') or [])} / "
              f"vendor {len(usage.get('vendors') or [])}")
        print(f"[test_main4] 이달 누계 ${usage.get('mtd_estimate_usd', 0):,.2f} "
              f"({usage.get('mtd_jobs', 0)}건)")

    from monitor_v2.hyperun.report import _build_main4, send_main4_report

    print()
    show(_build_main4(usage, report_date))
    print()

    if args.dry_run:
        print('[test_main4] --dry-run 이라 보내지 않았다')
        return 0

    send_main4_report(usage, report_date)
    print('[test_main4] 완료 — Slack 채널에서 본문과 스레드 넷을 확인하십시오')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
