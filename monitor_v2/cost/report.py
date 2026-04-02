"""
monitor_v2/cost/report.py

Main 1 메시지 + 스레드 3개를 Block Kit으로 순차 발송한다.

발송 순서:
    1. Main 1   — 전체 비용 요약 + Top 5 서비스
    2. Thread 1 — 계정 전체 서비스 비용 목록
    3. Thread 2 — IAM User별 비용 분석 (aws:createdBy)
    4. Thread 3 — 서비스별 리전 상세 (EC2 포함 전체 서비스)

환경변수:
    ACCOUNT_NAME: AWS 계정 별칭 (표시용)
"""

import os
from datetime import date, timedelta
from slack_sdk.models.blocks import (
    HeaderBlock,
    SectionBlock,
    DividerBlock,
    ContextBlock,
)
from slack_sdk.models.blocks.basic_components import MarkdownTextObject, PlainTextObject
from ..slack import client as slack

ACCOUNT_NAME = os.environ.get('ACCOUNT_NAME', 'hyu-ddps')
TOP_N        = 5

# EC2 관련 서비스명 집합 (ec2/report.py에서도 참조)
EC2_SERVICES = {
    'Amazon Elastic Compute Cloud - Compute',
    'Amazon EC2',
    'EC2 - Other',
}


# ---------------------------------------------------------------------------
# Block Kit 헬퍼
# ---------------------------------------------------------------------------

_MAX_SECTION = 2900   # Slack 제한 3000자에서 여유분 100자 확보
_CODE_OH     = 8      # "```\n" (4자) + "\n```" (4자) 오버헤드


def _header(text: str) -> HeaderBlock:
    """굵고 큰 헤더. HeaderBlock은 plain_text 전용이며 150자 제한."""
    return HeaderBlock(text=PlainTextObject(text=text[:150]))


def _section(text: str) -> SectionBlock:
    """mrkdwn 텍스트 섹션. 코드블록(```) 포함 가능, 3000자 제한."""
    return SectionBlock(text=MarkdownTextObject(text=text))


def _divider() -> DividerBlock:
    """섹션 간 구분선."""
    return DividerBlock()


def _context(text: str) -> ContextBlock:
    """보조 설명용 작은 mrkdwn 텍스트. ContextBlock은 elements 리스트를 받는다."""
    return ContextBlock(elements=[MarkdownTextObject(text=text)])


def _code_sections(title: str, lines: list) -> list:
    """
    제목 SectionBlock + 줄 목록을 코드블록(```)으로 감싼 SectionBlock 리스트를 반환한다.
    내용이 _MAX_SECTION을 초과하면 줄 단위로 분할해 여러 SectionBlock을 생성한다.
    """
    result    = [_section(title)]
    chunk     = []
    chunk_len = _CODE_OH

    for line in lines:
        cost = len(line) + 1  # +1 for '\n'
        if chunk and chunk_len + cost > _MAX_SECTION:
            result.append(_section("```\n" + "\n".join(chunk) + "\n```"))
            chunk     = []
            chunk_len = _CODE_OH
        chunk.append(line)
        chunk_len += cost

    if chunk:
        result.append(_section("```\n" + "\n".join(chunk) + "\n```"))

    return result


# ---------------------------------------------------------------------------
# 공통 계산 / 포맷 헬퍼
# ---------------------------------------------------------------------------

def calc_change(today: float, compare: float) -> tuple:
    """(delta, pct|None). compare=0이면 pct=None."""
    delta = today - compare
    pct   = (delta / compare * 100.0) if compare else None
    return delta, pct


def fmt_change(delta: float, pct) -> str:
    """'+$13.45 (+12.2% [UP])' 형식 문자열."""
    sign  = '+' if delta >= 0 else ''
    d_str = f"{sign}${abs(delta):,.2f}" if delta >= 0 else f"-${abs(delta):,.2f}"

    if pct is None:
        suffix = '(신규)' if delta > 0 else '(중단)' if delta < 0 else ''
    else:
        s     = '+' if pct >= 0 else ''
        suffix = f"({s}{pct:.1f}%)"

    return f"{d_str} {suffix}".strip()


def _top_n(costs: dict, n: int = TOP_N) -> list:
    return sorted(
        [(s, c) for s, c in costs.items() if c > 0],
        key=lambda x: x[1], reverse=True
    )[:n]


# ---------------------------------------------------------------------------
# Main 1
# ---------------------------------------------------------------------------

def _build_main1(
    d1_date: date,
    daily_d1: dict,
    daily_d2: dict,
    daily_lm: dict,
    mtd_this: float,
    mtd_prev: float,
    forecast: float = 0.0,
) -> list:
    total_d1 = sum(daily_d1.values())
    total_d2 = sum(daily_d2.values())
    d2_date  = d1_date - timedelta(days=1)

    d_yday, p_yday = calc_change(total_d1, total_d2)
    d_mtd,  p_mtd  = calc_change(mtd_this, mtd_prev)
    projected      = mtd_this + forecast

    # ── 일일 비용 ─────────────────────────────────────────────────────────
    daily_lines = [
        f"{d1_date}:   ${total_d1:,.2f}",
        f"{d2_date}:   ${total_d2:,.2f}   {fmt_change(d_yday, p_yday)}",
    ]

    # ── 월 누계 ───────────────────────────────────────────────────────────
    mtd_lines = [
        f"이번달 누계:    ${mtd_this:,.2f}",
        f"전월 동기 누계:      ${mtd_prev:,.2f}   {fmt_change(d_mtd, p_mtd)}",
    ]
    if forecast > 0:
        mtd_lines.append(f"이달 예상:      ${projected:>,.2f}   (잔여 예측 +${forecast:,.2f})")
    else:
        mtd_lines.append("이달 예상:      (현재 날짜가 아닌 경우, 예측 데이터 확인 불가)")

    # ── Top N ─────────────────────────────────────────────────────────────
    top5       = _top_n(daily_d1)
    top5_lines = []
    for rank, (service, cost) in enumerate(top5, 1):
        d1, p1 = calc_change(cost, daily_d2.get(service, 0.0))
        d2, p2 = calc_change(cost, daily_lm.get(service, 0.0))
        top5_lines.append(
             f"{rank}. {service:<30}  "
            f"${cost:,.2f}  "
            f"어제대비 {fmt_change(d1, p1)}  "
            f"전월동기대비 {fmt_change(d2, p2)}\n"
        )

    return [
        _header(f"AWS Cost Report  |  {d1_date}  |  {ACCOUNT_NAME}"),
        _divider(),
        *_code_sections("*[ 일일 비용 ]*", daily_lines),
        _divider(),
        *_code_sections("*[ 월 누계 ]*", mtd_lines),
        _divider(),
        *_code_sections(f"*[ Top {TOP_N} 서비스 {d1_date} ]*", top5_lines),
        _divider(),
        _context("자세한 내용은 스레드에서 확인하세요."),
    ]


# ---------------------------------------------------------------------------
# Thread 1: 전체 서비스 비용 목록
# ---------------------------------------------------------------------------

def _build_thread1(d1_date: date, daily_d1: dict, daily_d2: dict) -> list:
    total_d1 = sum(daily_d1.values())
    total_d2 = sum(daily_d2.values())
    d, p     = calc_change(total_d1, total_d2)

    summary = (
        f"{d1_date} 합계:   ${total_d1:,.2f}\n"
        f"{d1_date - timedelta(days=1)} 합계:   ${total_d2:,.2f}   ({fmt_change(d, p)})"
    )

    svc_lines = [
        f"{service:<42}  ${cost:>9,.4f}"
        for service, cost in sorted(daily_d1.items(), key=lambda x: x[1], reverse=True)
        if cost > 0
    ]

    return [
        _header(f"Thread 1  |  서비스별 전체 비용  |  {ACCOUNT_NAME}"),
        _divider(),
        *_code_sections("*[ 합계 ]*", summary.splitlines()),
        _divider(),
        *_code_sections("*[ 서비스 목록 ]*", svc_lines),
    ]


# ---------------------------------------------------------------------------
# Thread 2: IAM User별 비용 분석
# ---------------------------------------------------------------------------

def _creator_rollup(data: dict) -> tuple:
    """
    {service: {creator: float}} → (creator_totals, creator_services) 두 dict 반환.
    """
    totals   = {}
    services = {}
    for service, creators in data.items():
        for creator, cost in creators.items():
            totals[creator] = totals.get(creator, 0.0) + cost
            services.setdefault(creator, {})
            services[creator][service] = services[creator].get(service, 0.0) + cost
    return totals, services


def _fmt_creator_lines(totals: dict, services: dict) -> list:
    """creator별 합계 + 서비스 드릴다운 줄 목록 반환."""
    lines = []
    for creator, total in sorted(totals.items(), key=lambda x: x[1], reverse=True):
        if total <= 0:
            continue
        short    = creator.split(':')[-1] if creator.startswith('IAMUser') else creator
        lines.append(f"{short:<42}  ${total:,.2f}")
        svc_list = sorted(services[creator].items(), key=lambda x: x[1], reverse=True)
        last_idx = len([c for _, c in svc_list if c > 0]) - 1
        for idx, (svc, cost) in enumerate(svc_list):
            if cost <= 0:
                continue
            branch = "└─" if idx == last_idx else "├─"
            lines.append(f"   {branch} {svc:<35}  ${cost:,.2f}")
        lines.append("")
    return lines


def _build_thread2(by_creator: dict, by_creator_mtd: dict, forecast: float) -> list:
    d1_totals,  d1_svc  = _creator_rollup(by_creator)
    mtd_totals, mtd_svc = _creator_rollup(by_creator_mtd)
    total_mtd            = sum(mtd_totals.values())

    # ── D-1 ──────────────────────────────────────────────────────────────
    d1_lines = _fmt_creator_lines(d1_totals, d1_svc) or ["(데이터 없음)"]

    # ── MTD ──────────────────────────────────────────────────────────────
    mtd_lines = []
    if not mtd_totals:
        mtd_lines.append("(데이터 없음)")
    else:
        for creator, mtd_total in sorted(mtd_totals.items(), key=lambda x: x[1], reverse=True):
            if mtd_total <= 0:
                continue
            short = creator.split(':')[-1] if creator.startswith('IAMUser') else creator
            if forecast > 0 and total_mtd > 0:
                creator_fc = forecast * (mtd_total / total_mtd)
                projected  = mtd_total + creator_fc
                fstr = f"   -> 이달 예상 ${projected:,.2f} (+${creator_fc:,.2f} 추정)"
            else:
                fstr = ""
            mtd_lines.append(f"{short:<42}  MTD: ${mtd_total:,.2f}{fstr}")
            for svc, cost in sorted(mtd_svc[creator].items(), key=lambda x: x[1], reverse=True):
                if cost > 0:
                    mtd_lines.append(f"   ├─ {svc:<35}  ${cost:,.2f}")
            mtd_lines.append("")

    blocks = [
        _header("Thread 2  |  IAM User별 비용 분석"),
        _divider(),
        *_code_sections("*[ 당일 ]*", d1_lines),
        _divider(),
        *_code_sections("*[ 당월 누계 (MTD) ]*", mtd_lines),
    ]

    if forecast <= 0:
        blocks.append(_context("* 잔여 예측 없음 — 현재 날짜가 아니므로 CE forecasst API 요청 실패"))

    return blocks


# ---------------------------------------------------------------------------
# Thread 3: 서비스별 리전 상세
# ---------------------------------------------------------------------------

def _fmt_region_lines_d1(service_region_dict: dict, d2_ref: dict) -> list:
    filtered    = {s: r for s, r in service_region_dict.items() if sum(r.values()) > 0}
    sorted_svcs = sorted(filtered.items(), key=lambda x: sum(x[1].values()), reverse=True)
    lines = []
    for service, regions in sorted_svcs:
        total   = sum(regions.values())
        d, p    = calc_change(total, d2_ref.get(service, 0.0))
        lines.append(f"{service:<37}  ${total:>8,.2f}   {fmt_change(d, p)} vs 어제")
        for region, cost in sorted(regions.items(), key=lambda x: x[1], reverse=True):
            if cost > 0:
                lines.append(f"   ├─ {region:<25}  ${cost:>8,.2f}")
        lines.append("")
    return lines or ["(데이터 없음)"]


def _fmt_region_lines_mtd(service_region_dict: dict, forecast: float) -> list:
    filtered    = {s: r for s, r in service_region_dict.items() if sum(r.values()) > 0}
    sorted_svcs = sorted(filtered.items(), key=lambda x: sum(x[1].values()), reverse=True)
    total_mtd   = sum(sum(r.values()) for r in filtered.values())
    lines = []
    for service, regions in sorted_svcs:
        svc_mtd = sum(regions.values())
        if forecast > 0 and total_mtd > 0:
            svc_fc    = forecast * (svc_mtd / total_mtd)
            projected = svc_mtd + svc_fc
            fc_str    = f"   -> 이달 예상 ${projected:,.2f} (+${svc_fc:,.2f} 추정)"
        else:
            fc_str = ""
        lines.append(f"{service:<37}  MTD: ${svc_mtd:>8,.2f}{fc_str}")
        for region, cost in sorted(regions.items(), key=lambda x: x[1], reverse=True):
            if cost > 0:
                lines.append(f"   ├─ {region:<25}  ${cost:>8,.2f}")
        lines.append("")
    return lines or ["(데이터 없음)"]


def _build_thread3(
    by_region:     dict,
    by_region_mtd: dict,
    daily_d2:      dict,
    forecast:      float,
    d1_date:       date = None,
) -> list:
    d1_lines  = _fmt_region_lines_d1(by_region, daily_d2)
    mtd_lines = _fmt_region_lines_mtd(by_region_mtd, forecast)

    d1_label = str(d1_date) if d1_date else "당일"
    blocks = [
        _header("Thread 3  |  서비스별 리전 상세"),
        _divider(),
        *_code_sections(f"*[ {d1_label} ]*", d1_lines),
        _divider(),
        *_code_sections("*[ 당월 누계 (MTD) ]*", mtd_lines),
    ]

    if forecast <= 0:
        blocks.append(_context("* 잔여 예측 없음 — 현재 날짜가 아니므로 CE forecasst API 요청 실패 "))

    return blocks


# ---------------------------------------------------------------------------
# 진입점
# ---------------------------------------------------------------------------

def send_main1_report(cost_data: dict) -> None:
    """
    Main 1 + 스레드 3개를 Block Kit으로 순차 발송한다.

    Args:
        cost_data: cost/data.py collect_all()의 반환값
    """
    d1_date    = cost_data['d1_date']
    daily_d1   = cost_data['daily_d1']
    daily_d2   = cost_data['daily_d2']
    daily_lm   = cost_data['daily_lm']
    by_creator = cost_data['by_creator']
    by_region  = cost_data['by_region']
    mtd_this   = cost_data['mtd_this']
    mtd_prev   = cost_data['mtd_prev']

    by_creator_mtd = cost_data.get('by_creator_mtd', {})
    by_region_mtd  = cost_data.get('by_region_mtd', {})
    forecast       = cost_data.get('forecast', 0.0)

    main_ts = slack.post_blocks(
        _build_main1(d1_date, daily_d1, daily_d2, daily_lm, mtd_this, mtd_prev, forecast),
        fallback_text=f"AWS Cost Report {d1_date} / {ACCOUNT_NAME}",
    )

    slack.post_blocks(
        _build_thread1(d1_date, daily_d1, daily_d2),
        fallback_text="Thread 1: 서비스별 전체 비용",
        thread_ts=main_ts,
    )
    slack.post_blocks(
        _build_thread2(by_creator, by_creator_mtd, forecast),
        fallback_text="Thread 2: IAM User별 비용 분석",
        thread_ts=main_ts,
    )
    slack.post_blocks(
        _build_thread3(by_region, by_region_mtd, daily_d2, forecast, d1_date),
        fallback_text="Thread 3: 서비스별 리전 상세",
        thread_ts=main_ts,
    )
