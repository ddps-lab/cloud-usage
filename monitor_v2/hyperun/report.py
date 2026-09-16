"""
monitor_v2/hyperun/report.py

Main 4 메시지 + 스레드 3개를 Block Kit으로 순차 발송한다.

발송 순서:
    1. Main 4   — 당일 실행 + 비용(추정) + MTD + Top 5 사용자
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
    # ★ 당일과 어제. 당일은 아직 안 끝났고 그래서 이 숫자는 읽는 동안 커진다 —
    # 그게 job 을 지켜보는 사람이 보고 싶은 것이다. "무엇이 지금 돌고 있고 얼마를
    # 썼나" 가 이 보고서의 질문이지, "끝난 하루에 얼마를 썼나" 가 아니다.
    # 끝난 날은 스레드의 일별 표에 그대로 있다.
    d1 = _day(usage, 0)
    d2 = _day(usage, 1)
    d1_date = report_date

    spent_d1 = float(d1.get('estimate_usd') or 0.0)
    spent_d2 = float(d2.get('estimate_usd') or 0.0)
    delta, pct = calc_change(spent_d1, spent_d2)

    run_fields = [
        f"*job*\n`{int(d1.get('jobs') or 0)}건`",
        f"*GPU*\n`{float(d1.get('gpu_hours') or 0.0):,.1f} Hour`",
    ]

    # 어제 대비는 어제가 창 안에 있을 때만 말한다. 이달 1일이면 비교 대상이 없고,
    # 그때 "0%" 나 "▲ +100%" 를 찍으면 없는 사실을 만든 것이 된다.
    #
    # 그리고 당일은 아직 안 끝났으므로 어제 대비는 늘 낮게 나온다. 그래도 넣는 이유는
    # 자릿수가 다를 때를 보려는 것이다 — 오전에 이미 어제 총액을 넘겼으면 그건 봐야 한다.
    if d2:
        spend_today = (f"*당일 ({d1_date})*\n`${spent_d1:,.2f}` "
                       f"_어제 대비 {_arrow(delta)} {fmt_change(delta, pct)}_")
    else:
        spend_today = f"*당일 ({d1_date})*\n`${spent_d1:,.2f}`"

    mtd = float(usage.get('mtd_estimate_usd') or 0.0)
    mtd_hours = float(usage.get('mtd_gpu_hours') or 0.0)
    spend_fields = [
        spend_today,
        (f"*MTD*\n`${mtd:,.2f}` "
         f"_({int(usage.get('mtd_jobs') or 0)}건, {mtd_hours:,.1f} Hour)_"),
    ]

    # Top 사용자. 오늘 쓴 돈이 있는 사람만, 많이 쓴 순으로. 아무도 안 돌렸으면
    # 이 절은 통째로 빠지고 그게 정직한 화면이다.
    top_blocks = []
    spenders = sorted(
        (u for u in (usage.get('users') or []) if float(u.get('day_estimate_usd') or 0) > 0),
        key=lambda u: -float(u.get('day_estimate_usd') or 0))[:TOP_N]
    for rank, person in enumerate(spenders, 1):
        # 한 줄이다. 두 줄로 쓰면 다섯 명이 열 줄이 되고, Slack 에서 열 줄은
        # 스크롤이다 — 훑어보라고 만든 절이 훑어지지 않는다.
        top_blocks.append(_section(
            f"*{rank}. {person.get('name', '?')}* — "
            f"`${float(person.get('day_estimate_usd') or 0):,.2f}` "
            f"_(job {int(person.get('day_jobs') or 0)}건, "
            f"{float(person.get('day_gpu_hours') or 0):,.1f} GPU Hour)_"
        ))
    if not top_blocks:
        top_blocks = [_section("_오늘 아직 실행된 job 이 없습니다._")]

    # vendor 는 한 줄로 요약한다. 대개 둘셋이라 절을 따로 두면 자리만 먹는다.
    vendors = sorted(
        ((v.get('name', '?'), float(v.get('day_estimate_usd') or 0))
         for v in (usage.get('vendors') or [])),
        key=lambda pair: -pair[1])
    vendor_line = "  ·  ".join(f"{name} `${cost:,.2f}`"
                               for name, cost in vendors if cost > 0) or "_없음_"

    blocks = [
        _header(f"hyperun Job Report  |  {d1_date}  |  {ACCOUNT_NAME}"),
        _section("*[ 당일 실행 ]*"),
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

    # ★ 한 줄이고, 그 한 줄이 두 가지를 동시에 말한다: 무엇을 셌는지(hyperun 으로 낸
    # job)와 그래서 무엇과 다를 수 있는지(vendor 청구서). 앞 판에는 계산 근거와 실측
    # 사례까지 넣었는데, context 줄은 작은 글씨 한 줄이고 거기에 네 문장을 넣으면
    # 아무도 안 읽는다. 자세한 것은 gateway 의 /v1/usage 가 note 로 답한다.
    blocks.append(_context(
        "hyperun으로 제출한 job만 집계하니 vendor 청구서와 다를 수 있습니다."))
    return blocks


def _bucket_rows(buckets: list) -> list:
    """표 한 장. 이달 누계와 그날을 나란히 둔다.

    ★ "기간 합계" 가 아니라 MTD 다. 창의 길이는 구현이 정하는 값이라 30일이든 7일이든
    읽는 사람에게는 의미가 없고, "이달 얼마" 는 예산을 보는 사람이 실제로 쓰는 단위다.
    gateway 의 `mtd_*` 와 같은 뜻이 되도록 창을 이달 1일부터로 잡는다.
    """
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


_BUCKET_HEADERS = ["이름", "job", "GPU Hour", "MTD(추정)", "당일(추정)"]


def _build_thread(title: str, buckets: list, report_date: date) -> list:
    return [
        _header(f"{title}  |  {ACCOUNT_NAME}"),
        _divider(),
        *_table_section(f"*[ {report_date:%Y-%m} 이달 누계 ]*", _BUCKET_HEADERS,
                        _bucket_rows(buckets)),
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
        *_table_section("*[ 날짜별 ]*", ["날짜", "job", "GPU Hour", "추정"], rows),
        _context("job 이 자정을 넘기면 시간은 날짜별로 쪼개지고, job 건수는 시작한 날에 "
                 "한 번만 셉니다."),
    ]


def send_main4_report(usage: dict, report_date: date = None) -> None:
    """Main 4 와 스레드 셋을 보낸다.

    Args:
        usage: `hyperun.data.collect()` 가 돌려준 본문.
        report_date: 보고서의 기준일. 생략하면 오늘(UTC)이고, 본문의 "당일" 이 그날이다.

    스레드는 본문이 성공했을 때만 보낸다. 본문 없이 스레드만 뜨면 채널에 맥락 없는
    표가 세 장 남는다.
    """
    today = report_date or datetime.now(timezone.utc).date()

    thread_ts = slack.post_blocks(_build_main4(usage, today),
                                  fallback_text='hyperun Job Report')
    if not thread_ts:
        return
    for title, key in (('Thread 1  |  team 별', 'teams'),
                       ('Thread 2  |  user 별', 'users'),
                       ('Thread 3  |  vendor 별', 'vendors')):
        slack.post_blocks(_build_thread(title, usage.get(key) or [], today),
                          fallback_text=title, thread_ts=thread_ts)
    slack.post_blocks(_build_thread_days(usage),
                      fallback_text='일별 추이', thread_ts=thread_ts)
