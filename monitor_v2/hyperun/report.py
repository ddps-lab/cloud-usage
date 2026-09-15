"""
monitor_v2/hyperun/report.py

Main 4 메시지 + 스레드 3개를 Block Kit으로 순차 발송한다.

발송 순서:
    1. Main 4   — 어제 실행 + 비용(추정) + 이달 누계 + Top 5 사용자
    2. Thread 1 — team 별
    3. Thread 2 — user 별
    4. Thread 3 — vendor 별, 그리고 일별 추이

Main 1 과 같은 모양을 쓴다:
    _header -> _section("*[ ... ]*") -> _fields_section -> _divider -> 항목별
    _section. Main 1 이 표를 쓰지 않는 이유와 같다 — 본문은 눈으로 훑는 자리이고
    표는 스레드에서 찾아보는 자리다. 그래서 여기서도 표는 스레드에만 있다.

★ 여기 숫자는 전부 추정이다:
    카탈로그 단가 x 시간이다. vendor 청구서는 다른 숫자이고 실측으로 크게 달랐다 —
    baseline-c 가 계산 $11.07, 청구 $44.28. 그래서 제목과 본문에 "추정" 을 적고,
    맨 아래 context 에 한 번 더 적는다. 읽는 사람이 이걸 청구서로 알고 대조하다
    반나절을 잃는 것이 이 보고서가 낼 수 있는 가장 나쁜 결과다.

★★ hyperun 으로 낸 job 만 센다:
    gateway 가 PacsJob 에서만 읽으므로 같은 계정에 누가 손으로 띄운 pod 은 없다.

환경변수:
    ACCOUNT_NAME: 표시용 별칭 (Main 1 과 같은 것을 쓴다)
"""

import os
from datetime import date, datetime, timedelta, timezone

from ..slack import client as slack
from ..utils.blocks import (
    header as _header, section as _section, divider as _divider,
    context as _context, table_section as _table_section,
    fields_section as _fields_section, calc_change, fmt_change,
)

ACCOUNT_NAME = os.environ.get('ACCOUNT_NAME', 'hyu-ddps')
TOP_N = 5


def _arrow(delta: float) -> str:
    return "▲" if delta >= 0 else "▼"


def _day(usage: dict, offset: int) -> dict:
    """뒤에서 offset 번째 날. 0 이면 마지막 날.

    days 는 오래된 것부터 정렬되어 온다. 창이 그만큼 길지 않으면 빈 날을 돌려주는데,
    0 을 돌려주는 것보다 낫다: 0 은 "그날 아무것도 안 돌았다" 는 뜻이고, 없는 날은
    "모른다" 이기 때문이다. 다만 화면에서는 둘 다 $0.00 으로 보이므로, 창이 짧아서
    비교가 안 되는 경우는 아래에서 문구로 가른다.
    """
    days = usage.get('days') or []
    index = len(days) - 1 - offset
    return days[index] if 0 <= index < len(days) else {}


def _build_main4(usage: dict, report_date: date) -> list:
    """본문 하나. 표 없이, Main 1 과 같은 순서로."""
    # 어제와 그제. 오늘은 아직 안 끝났으므로 본문이 말하는 "어제" 는 마지막으로
    # 완결된 날이다. 오늘 숫자를 어제라고 부르면 읽는 동안 커지는 숫자가 된다.
    d1 = _day(usage, 1)
    d2 = _day(usage, 2)
    d1_date = report_date - timedelta(days=1)

    spent_d1 = float(d1.get('estimate_usd') or 0.0)
    spent_d2 = float(d2.get('estimate_usd') or 0.0)
    delta, pct = calc_change(spent_d1, spent_d2)

    run_fields = [
        f"*job*\n`{int(d1.get('jobs') or 0)}건`",
        f"*GPU 시간*\n`{float(d1.get('gpu_hours') or 0.0):,.1f}시간`",
    ]

    # 어제 대비는 그제가 창 안에 있을 때만 말한다. 창이 하루뿐이면 비교 대상이
    # 없는 것이고, 그때 "0%" 나 "▲ +100%" 를 찍으면 없는 사실을 만든 것이 된다.
    if d2:
        spend_yesterday = (f"*어제 ({d1_date})*\n`${spent_d1:,.2f}` "
                           f"_{_arrow(delta)} {fmt_change(delta, pct)}_")
    else:
        spend_yesterday = f"*어제 ({d1_date})*\n`${spent_d1:,.2f}`"

    mtd = float(usage.get('mtd_estimate_usd') or 0.0)
    mtd_hours = float(usage.get('mtd_gpu_hours') or 0.0)
    spend_fields = [
        spend_yesterday,
        (f"*이번달 누계*\n`${mtd:,.2f}` "
         f"_({int(usage.get('mtd_jobs') or 0)}건, {mtd_hours:,.1f}시간)_"),
    ]

    # Top 사용자. 어제 쓴 돈이 있는 사람만, 많이 쓴 순으로. 어제 아무도 안 돌렸으면
    # 이 절은 통째로 빠지고 그게 정직한 화면이다.
    top_blocks = []
    spenders = sorted(
        (u for u in (usage.get('users') or []) if float(u.get('day_estimate_usd') or 0) > 0),
        key=lambda u: -float(u.get('day_estimate_usd') or 0))[:TOP_N]
    for rank, person in enumerate(spenders, 1):
        top_blocks.append(_section(
            f"*{rank}. {person.get('name', '?')}* — "
            f"`${float(person.get('day_estimate_usd') or 0):,.2f}`\n"
            f"job {int(person.get('day_jobs') or 0)}건, "
            f"{float(person.get('day_gpu_hours') or 0):,.1f} GPU시간"
        ))
    if not top_blocks:
        top_blocks = [_section("_어제 실행된 job 이 없습니다._")]

    # vendor 는 한 줄로 요약한다. 대개 둘셋이라 절을 따로 두면 자리만 먹는다.
    vendors = sorted(
        ((v.get('name', '?'), float(v.get('day_estimate_usd') or 0))
         for v in (usage.get('vendors') or [])),
        key=lambda pair: -pair[1])
    vendor_line = "  ·  ".join(f"{name} `${cost:,.2f}`"
                               for name, cost in vendors if cost > 0) or "_없음_"

    blocks = [
        _header(f"hyperun Job Report  |  {d1_date}  |  {ACCOUNT_NAME}"),
        _section("*[ 어제 실행 ]*"),
        _fields_section(run_fields),
        _divider(),
        _section("*[ 비용 (추정) ]*"),
        _fields_section(spend_fields),
        _divider(),
        _section(f"*[ vendor 별  {d1_date} ]*"),
        _section(vendor_line),
        _divider(),
        _section(f"*[ Top {TOP_N} 사용자  {d1_date} ]*"),
        *top_blocks,
        _divider(),
    ]

    # ★ 가격에 대한 주의는 마지막 한 줄에 반드시 남는다. 위의 숫자만 보고 청구서와
    # 맞추려는 사람을 막는 것이 이 줄의 전부다.
    caveat = ("모든 금액은 카탈로그 단가 x 시간의 **추정**입니다. vendor 청구서와 다릅니다"
              "(실측: 계산 $11.07 대 청구 $44.28). hyperun 으로 낸 job 만 집계합니다.")
    unpriced = sum(int(d.get('unpriced_jobs') or 0) for d in (usage.get('days') or []))
    if unpriced:
        caveat = (f"★ 가격을 모르는 기계에서 돈 job 이 {unpriced}건 있어 합계가 "
                  f"그만큼 **적습니다**. " + caveat)
    blocks.append(_context(caveat))
    return blocks


def _bucket_rows(buckets: list) -> list:
    """표 한 장. 창 전체와 어제를 나란히 둔다."""
    rows = []
    for b in buckets:
        rows.append([
            str(b.get('name', '?')),
            f"{int(b.get('jobs') or 0)}",
            f"{float(b.get('gpu_hours') or 0):,.1f}",
            f"${float(b.get('estimate_usd') or 0):,.2f}",
            f"${float(b.get('day_estimate_usd') or 0):,.2f}",
        ])
    return rows


_BUCKET_HEADERS = ["이름", "job", "GPU시간", "기간 합계(추정)", "어제(추정)"]


def _build_thread(title: str, buckets: list, window_days: int) -> list:
    return [
        _header(f"{title}  |  {ACCOUNT_NAME}"),
        _divider(),
        *_table_section(f"*[ 최근 {window_days}일 ]*", _BUCKET_HEADERS, _bucket_rows(buckets)),
    ]


def _build_thread_days(usage: dict) -> list:
    rows = [
        [str(d.get('date', '')),
         f"{int(d.get('jobs') or 0)}",
         f"{float(d.get('gpu_hours') or 0):,.1f}",
         f"${float(d.get('estimate_usd') or 0):,.2f}"]
        for d in (usage.get('days') or [])
        # 아무것도 안 돈 날은 뺀다. 30일 창에서 빈 줄 스무 개는 표를 못 읽게 한다.
        if (d.get('jobs') or d.get('gpu_hours'))
    ]
    return [
        _header(f"일별 추이  |  {ACCOUNT_NAME}"),
        _divider(),
        *_table_section("*[ 날짜별 ]*", ["날짜", "job", "GPU시간", "추정"], rows),
        _context("job 이 자정을 넘기면 시간은 날짜별로 쪼개지고, job 건수는 시작한 날에 "
                 "한 번만 셉니다."),
    ]


def send_main4_report(usage: dict, report_date: date = None) -> None:
    """Main 4 와 스레드 셋을 보낸다.

    Args:
        usage: `hyperun.data.collect()` 가 돌려준 본문.
        report_date: 보고서의 기준일. 생략하면 오늘(UTC)이고, 본문이 말하는
            "어제" 는 그 하루 전이다.

    스레드는 본문이 성공했을 때만 보낸다. 본문 없이 스레드만 뜨면 채널에 맥락 없는
    표가 세 장 남는다.
    """
    today = report_date or datetime.now(timezone.utc).date()
    window = len(usage.get('days') or [])

    thread_ts = slack.post_blocks(_build_main4(usage, today),
                                  fallback_text='hyperun Job Report')
    if not thread_ts:
        return
    for title, key in (('Thread 1  |  team 별', 'teams'),
                       ('Thread 2  |  user 별', 'users'),
                       ('Thread 3  |  vendor 별', 'vendors')):
        slack.post_blocks(_build_thread(title, usage.get(key) or [], window),
                          fallback_text=title, thread_ts=thread_ts)
    slack.post_blocks(_build_thread_days(usage),
                      fallback_text='일별 추이', thread_ts=thread_ts)
