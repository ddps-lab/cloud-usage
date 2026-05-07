"""
monitor_v2/cost/report_analysis.py

Main 3 메시지를 Slack에 발송한다.
채널 메시지 1개 (스레드 없음).

구성:
    헤더        — "AWS 비용 변화 분석 | {d1_date} | {account}"
    요약 수치   — 어제 총비용 / 이번 달 누계(N일) / 월말 예상
    Q9  테이블  — 서비스별 비용 변화 Top
    Q10 테이블  — 리소스 타입별 비용 변화 Top
    Q11 테이블  — 리소스 ID별 비용 변화 Top
    AI 요약     — Nova Micro 요약 텍스트
    context     — 분석 기준 날짜 / 데이터 소스 / 모델
"""

import os
from datetime import date

from ..slack import client as slack
from ..utils.blocks import (
    header as _header, section as _section, divider as _divider,
    fields_section as _fields_section, context as _context,
    table_section as _table_section,
)
from .analysis import collect_all

ACCOUNT_NAME      = os.environ.get('ACCOUNT_NAME')
_BEDROCK_MODEL_ID = os.environ.get('BEDROCK_MODEL_ID', 'amazon.nova-micro-v1:0')


def _fmt_diff(v: float) -> str:
    sign = "▲" if v >= 0 else "▼"
    return f"{sign} {'+' if v >= 0 else ''}{v:,.2f}"


def _service_rows(rows: list) -> list:
    return [
        [r['service'], f"${r['cost_d1']:,.2f}", f"${r['cost_d2']:,.2f}", _fmt_diff(r['diff'])]
        for r in rows
    ] or [["(데이터 없음)", "", "", ""]]


def _usage_type_rows(rows: list) -> list:
    return [
        [r['service'], r['usage_type'], f"${r['cost_d1']:,.2f}", f"${r['cost_d2']:,.2f}", _fmt_diff(r['diff'])]
        for r in rows
    ] or [["(데이터 없음)", "", "", "", ""]]


def _resource_rows(rows: list) -> list:
    return [
        [r['service'], r['usage_type'], r['resource_id'],
         r.get('iam_user') or '-',
         f"${r['cost_d1']:,.2f}", f"${r['cost_d2']:,.2f}", _fmt_diff(r['diff'])]
        for r in rows
    ] or [["(데이터 없음)", "", "", "", "", "", ""]]


def _build_main3(analysis: dict) -> list:
    d1_date          = analysis['d1_date']
    d1_total         = analysis['d1_total']
    summary          = analysis['summary']
    service_rows     = analysis['service_rows']
    usage_rows       = analysis['usage_type_rows']
    resource_rows    = analysis['resource_rows']
    mtd_total        = analysis.get('mtd_total', 0.0)
    mtd_days_elapsed = analysis.get('mtd_days_elapsed', 0)
    forecast_total   = analysis.get('forecast_total', 0.0)

    fields = [
        f"*어제({d1_date}) 총비용*\n`${d1_total:,.2f}`",
        (
            f"*이번 달 누계 ({mtd_days_elapsed}일 경과)*\n`${mtd_total:,.2f}`"
            if mtd_total > 0 else "*이번 달 누계*\n`데이터 없음`"
        ),
        (
            f"*월말 예상*\n`${forecast_total:,.2f}`"
            if forecast_total > 0 else "*월말 예상*\n`예측 불가`"
        ),
    ]

    return [
        _header(f"AWS 비용 변화 분석  |  {d1_date}  |  {ACCOUNT_NAME}"),
        _fields_section(fields),
        _divider(),
        _section("*AI 요약*"),
        _section(summary),
        _divider(),
        *_table_section(
            f"*[ 서비스별 비용 변화 Top {len(service_rows)} ]*",
            ["서비스", "어제", "그제", "변화"],
            _service_rows(service_rows),
        ),
        _divider(),
        *_table_section(
            f"*[ 리소스 타입별 비용 변화 Top {len(usage_rows)} ]*",
            ["서비스", "타입", "어제", "그제", "변화"],
            _usage_type_rows(usage_rows),
        ),
        _divider(),
        *_table_section(
            f"*[ 리소스 ID별 비용 변화 Top {len(resource_rows)} ]*",
            ["서비스", "타입", "리소스 ID", "생성자", "어제", "그제", "변화"],
            _resource_rows(resource_rows),
        ),
        _context(
            f"분석 기준: {d1_date}  |  데이터 소스: CUR  |  모델: {_BEDROCK_MODEL_ID}"
        ),
    ]


def send_main3_report(d1_date: date) -> None:
    """
    Main 3 발송. mtd_this / forecast 는 collect_all 내부에서 수집한다.

    Args:
        d1_date: 리포트 기준일
    """
    analysis = collect_all(d1_date)
    slack.post_blocks(
        _build_main3(analysis),
        fallback_text=f"AWS 비용 변화 분석 {d1_date} / {ACCOUNT_NAME}",
    )
