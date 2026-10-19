"""
monitor_v2/vendor/report.py

Main 5 (neocloud vendor 가 실제로 청구한 금액) 본문 + 스레드 둘을 Block Kit 으로 발송한다.

발송 순서:
    1. Main 5   — 청구 표(vendor 별 오늘 / 이달 / 누적 + 합계)와 잔액 표
    2. Thread 1 — Shadeform: 지금 도는 instance, GPU 종류별, instance 별
    3. Thread 2 — RunPod: 지금 도는 pod, 그리고 GPU 종류별

★★★ 두 스레드 다 **지금 도는 것이 맨 위**다. 이달 집계는 지나간 일이고, 지금 돈이
    나가고 있는 기계가 먼저 보여야 한다. 본문 잔액 칸의 시간당 값이 어디서 오는지도
    그 표다.

왜 Main 4 가 아니라 따로인가:
    Main 4 는 hyperun 이 낸 PacsJob 만 세고 금액이 추정이다. 여기 숫자는 vendor 가
    실제로 청구한 금액이고 계정 전체다. 출처도 갈린다 — Main 4 는 hyperun gateway
    하나를 부르고, 이것은 RunPod 의 GraphQL 과 Shadeform 의 REST 를 부른다. 그래서
    gateway 가 답하지 않는 날에도 이 보고서는 나가야 하고, Main 4 의 스레드로 붙어
    있으면 본문이 없는 순간 같이 사라진다.

★ 본문에 표를 쓴다. Main 1 과 Main 4 는 본문에 표를 안 쓰는데 여기는 다르게 간다 —
    앞 판에서 vendor 마다 한 줄씩 늘어놓았더니 숫자 여섯 개가 문장 안에 섞여서 못
    읽겠다는 지적을 받았다(2026-09-18). 여기 본문은 거의 전부가 숫자이고, 열이 맞아야
    vendor 끼리 비교가 된다.

★★ 표는 둘로 나눈다. 앞의 것은 "얼마 썼나"(오늘 / 이달 / 누적), 뒤의 것은 "얼마
    남았나"(잔액 / 시간당 / 남은 시간)다. 한 표에 일곱 열을 넣으면 좁은 화면에서
    가로로 밀린다.

환경변수:
    ACCOUNT_NAME: 표시용 별칭 (Main 1, Main 4 와 같은 것을 쓴다)
"""

import os
from datetime import date

from ..slack import client as slack
from ..utils.blocks import (
    header as _header, divider as _divider, section as _section,
    table_section as _table_section,
)

ACCOUNT_NAME = os.environ.get('ACCOUNT_NAME', 'hyu-ddps')

# 값이 없는 칸. 0 과 구별되어야 한다 — 0 은 "안 썼다" 이고 이것은 "모른다" 다.
_BLANK = "-"


def _money(value) -> str:
    return f"${float(value or 0.0):,.2f}" if value is not None else _BLANK


def _duration(seconds: int) -> str:
    """26296 -> `7h18m`. 초까지 쓰면 표가 넓어지고 읽는 사람이 쓰는 단위가 아니다."""
    seconds = int(seconds or 0)
    return f"{seconds // 3600}h{seconds % 3600 // 60:02d}m"


def _find(vendors: list, name: str) -> dict:
    for vendor in vendors:
        if vendor.get('name') == name:
            return vendor
    return {}


def _build_main5(billing: dict) -> list:
    """본문 하나. 표 둘이고 그 밖의 설명은 없다."""
    vendors = billing.get('vendors') or []
    today = billing.get('today', '')
    month = billing.get('month', '')

    # ── 표 1: 얼마 썼나 ────────────────────────────────────────
    spend_rows = []
    for vendor in vendors:
        # 조회가 실패한 vendor 는 0 이 아니라 사유를 적는다. 0 은 "안 썼다" 이고
        # 실패는 "모른다" 라서, 그 둘을 같은 칸에 쓰면 합계까지 못 믿게 된다.
        if vendor.get('error'):
            spend_rows.append([vendor.get('name', '?'),
                               f"조회 실패: {vendor['error']}", _BLANK, _BLANK])
            continue
        spend_rows.append([
            vendor.get('name', '?'),
            _money(vendor.get('today_usd')),
            _money(vendor.get('mtd_usd')),
            _money(vendor.get('total_usd')),
        ])
    spend_rows.append(['**합계**',
                       _money(billing.get('total_today_usd')),
                       _money(billing.get('total_mtd_usd')),
                       _money(billing.get('total_all_usd'))])

    # ── 표 2: 얼마 남았나 ──────────────────────────────────────
    #
    # ★ 잔액에 합계 행을 두지 않는다. Shadeform 의 잔액으로 RunPod 의 청구를 낼 수
    # 없으므로, 합친 잔액을 합친 시간당으로 나누면 남은 시간이 실제보다 길게 나온다.
    # 2026-09-18 값으로 재면 합산이 223.0시간, RunPod 만 보면 198.6시간이었다 —
    # 24시간을 더 있다고 말하는 셈이고, 그 24시간이 밤사이 job 이 죽는 시간이다.
    balance_rows = []
    for vendor in vendors:
        if vendor.get('error'):
            continue
        balance = vendor.get('balance_usd')
        burn = vendor.get('burn_usd_per_hour')
        if balance is None:
            continue
        has_burn = burn is not None and float(burn) > 0
        balance_rows.append([
            vendor.get('name', '?'),
            _money(balance),
            f"${float(burn):,.3f}" if burn is not None else _BLANK,
            f"약 {float(balance) / float(burn):,.1f}시간" if has_burn else _BLANK,
        ])

    blocks = [
        _header(f"Neocloud Billing Report  |  {today}  |  {ACCOUNT_NAME}"),
        *_table_section(f"*[ 청구  UTC {today} ]*",
                        ["vendor", "오늘", f"이달 ({month})", "누적"],
                        spend_rows),
        _divider(),
    ]
    if balance_rows:
        blocks += _table_section("*[ 잔액 ]*",
                                 ["vendor", "잔액", "시간당", "남은 시간"],
                                 balance_rows)
    else:
        blocks.append(_section("*[ 잔액 ]*\n_잔액을 읽지 못했습니다._"))
    return blocks


def _build_thread_shadeform(billing: dict) -> list:
    """Shadeform: 지금 도는 instance, 이달 GPU 종류별, 이달 끝난 instance.

    ★ GPU 종류는 청구 API 가 아니라 `/v1/instances` 에서 온다. 그 둘이 같은 금액을
    말한다는 것은 확인했다 (`vendor/data.py` 의 `fold_shadeform`).

    ★★ 아래 두 표는 **끝난** instance 만 센다. Shadeform 은 instance 를 지울 때
    청구 행을 쓰는 것으로 보이므로(청구 41행이 전부 `status: deleted`), 지금 도는
    것은 이달 집계에도 본문 MTD 에도 아직 안 들어갔을 수 있다. 첫 표가 그 자리를
    메운다.
    """
    shadeform = _find(billing.get('vendors') or [], 'shadeform')
    month = billing.get('month', '')

    blocks = [
        _header(f"Shadeform  |  {ACCOUNT_NAME}"),
        _divider(),
    ]
    if shadeform.get('error'):
        blocks.append(_section(f"_조회 실패: {shadeform['error']}_"))
        return blocks

    # ★ 도는 것이 먼저다. 축이 청구가 아니라 시간당 가격인 이유는 방금 뜬 instance 의
    # 청구가 아직 0 이기 때문이다 (`vendor/data.py` 의 `fold_shadeform_instances`).
    live = shadeform.get('live') or []
    blocks += _table_section(
        "*[ 지금 도는 instance ]*",
        ["instance", "GPU", "sub-provider", "시간당", "지금까지", "시작 시각(UTC)"],
        [[row['name'], row['gpu'], row['provider'],
          _money(row['usd_per_hour']), _money(row['usd_so_far']), row['started']]
         for row in live]
        or [["_지금 도는 instance 가 없습니다._", "", "", "", "", ""]])

    blocks.append(_divider())

    gpu_rows = [[row['name'], f"{row['count']}개",
                 f"{row['seconds'] / 3600:,.1f}h", _money(row['usd'])]
                for row in (shadeform.get('by_gpu') or [])]
    blocks += _table_section(
        f"*[ GPU 종류별  {month} 이달 ]*",
        ["GPU", "instance", "시간", "청구"],
        gpu_rows or [["_이달 청구가 없습니다._", "", "", ""]])

    blocks.append(_divider())

    rows = [[row['name'], row['gpu'], row['provider'], _duration(row['seconds']),
             _money(row['usd']), row['ended']]
            for row in (shadeform.get('instances') or [])]
    blocks += _table_section(
        f"*[ 끝난 instance  {month} 이달 ]*",
        ["instance", "GPU", "sub-provider", "시간", "청구", "끝난 시각(UTC)"],
        rows or [["_이달 청구된 instance 가 없습니다._", "", "", "", "", ""]])
    return blocks


def _build_thread_runpod(billing: dict) -> list:
    """RunPod: 지금 도는 pod 목록과, 이달 GPU 종류별 청구.

    ★ "이달 실행 내역" 이 아니다. RunPod 은 끝난 pod 을 어떤 파라미터로도 안 주므로
    (`vendor/data.py` 머리말), 목록으로 만들 수 있는 것은 살아 있는 pod 뿐이다.
    """
    runpod = _find(billing.get('vendors') or [], 'runpod')
    month = billing.get('month', '')

    blocks = [
        _header(f"RunPod  |  {ACCOUNT_NAME}"),
        _divider(),
    ]
    if runpod.get('error'):
        blocks.append(_section(f"_조회 실패: {runpod['error']}_"))
        return blocks

    # ★ 제목이 "이달 실행 내역" 이 아니라 "지금 도는 pod" 인 것은 RunPod 이 끝난 pod 을
    # 어떤 경로로도 안 주기 때문이다 (`vendor/data.py` 의 `fold_runpod_pods`).
    # 제목에 없는 말을 쓰면 읽는 사람이 이 표를 이달 전체로 오해한다.
    pods = runpod.get('pods') or []
    blocks += _table_section(
        "*[ 지금 도는 pod ]*",
        ["pod", "GPU", "개수", "시간당", "시작 시각(UTC)"],
        [[row['name'], row['gpu'], str(row['count']),
          _money(row['usd_per_hour']), row['started']] for row in pods]
        or [["_지금 도는 pod 이 없습니다._", "", "", "", ""]])

    blocks.append(_divider())

    by_gpu = runpod.get('by_gpu') or []
    blocks += _table_section(
        f"*[ GPU 종류별  {month} 이달 ]*", ["GPU", "청구"],
        [[row['name'], _money(row['usd'])] for row in by_gpu]
        or [["_이달 청구가 없습니다._", ""]])
    return blocks


def send_main5_report(billing: dict, report_date: date = None) -> None:
    """Main 5 와 스레드 둘을 보낸다.

    Args:
        billing: `vendor.data.collect()` 가 돌려준 청구.
        report_date: 지금은 안 쓴다. 날짜는 `billing['today']` 가 들고 있고 그것이
            UTC 라서, 호출부가 KST 날짜를 넘겨도 표의 기준일이 흔들리지 않는다.

    스레드는 본문이 성공했을 때만 보낸다. 본문 없이 스레드만 뜨면 채널에 맥락 없는
    표가 두 장 남는다.
    """
    thread_ts = slack.post_blocks(_build_main5(billing),
                                  fallback_text='Neocloud Billing Report')
    if not thread_ts:
        return
    for title, blocks in (
            ('Thread 1  |  Shadeform', _build_thread_shadeform(billing)),
            ('Thread 2  |  RunPod', _build_thread_runpod(billing))):
        slack.post_blocks(blocks, fallback_text=title, thread_ts=thread_ts)
