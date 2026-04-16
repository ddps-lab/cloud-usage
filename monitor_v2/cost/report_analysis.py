"""
monitor_v2/cost/report_analysis.py

Main 3 메시지를 Slack에 발송한다.
채널 메시지 1개 (스레드 없음).

구성:
    헤더        — "AWS 비용 변화 분석 | {d1_date} | {account}"
    요약 수치   — 어제 총비용 / 전날 대비 변화
    Q9  테이블  — 서비스별 비용 변화 Top 10
    Q10 테이블  — 리소스 타입별 비용 변화 Top 10
    Q11 테이블  — 리소스 ID별 비용 변화 Top 10
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
_ANOMALY_SIGMA    = float(os.environ.get('ANOMALY_SIGMA', '1.5'))


def _fmt_diff(v: float) -> str:
    sign = "▲" if v >= 0 else "▼"
    return f"{sign} {'+' if v >= 0 else ''}{v:,.2f}"


def _avg_label(mu: float, is_anomaly: bool, hist_days: int) -> str:
    """단일 기간(7일 or 30일) 평균 대비 상태 레이블."""
    if hist_days < 7:
        return "히스토리 부족"
    status = "⚠ 이상치 ▲" if is_anomaly else "정상"
    return f"`${mu:,.2f}` → {status}"


def _lm_diff_label(d1_total: float, lm_total: float) -> str:
    if lm_total <= 0:
        return "전월 데이터 없음"
    diff = d1_total - lm_total
    pct  = diff / lm_total * 100
    sign = "▲" if diff >= 0 else "▼"
    return f"{sign} {abs(pct):.1f}%"


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
    d1_date        = analysis['d1_date']
    d2_date        = analysis['d2_date']
    d1_total       = analysis['d1_total']
    d2_total       = analysis['d2_total']
    diff           = d1_total - d2_total
    pct            = (diff / d2_total * 100) if d2_total else 0.0
    summary        = analysis['summary']
    service_rows   = analysis['service_rows']
    usage_rows     = analysis['usage_type_rows']
    resource_rows  = analysis['resource_rows']
    anomaly_stats  = analysis.get('anomaly_stats', {})
    lm_total       = analysis.get('lm_total', 0.0)
    lm_date        = analysis.get('lm_date')

    hist_days   = anomaly_stats.get('hist_days', 0) if anomaly_stats else 0
    lm_lbl      = _lm_diff_label(d1_total, lm_total)
    lm_date_str = str(lm_date) if lm_date else '-'

    lbl_7  = _avg_label(anomaly_stats.get('mu_7', 0),  anomaly_stats.get('is_anomaly_7', False),  hist_days)
    lbl_30 = _avg_label(anomaly_stats.get('mu_30', 0), anomaly_stats.get('is_anomaly_30', False), hist_days)

    fields = [
        f"*어제({d1_date}) 총비용*\n`${d1_total:,.2f}`",
        f"*전날({d2_date}) 대비*\n`{_fmt_diff(diff)}` `({pct:+.1f}%)`",
        f"*7일 평균 대비*\n{lbl_7}",
        f"*30일 평균 대비*\n{lbl_30}",
        f"*전월 동일일({lm_date_str}) 대비*\n`{lm_lbl}`",
    ]

    return [
        _header(f"AWS 비용 변화 분석  |  {d1_date}  |  {ACCOUNT_NAME}"),
        _fields_section(fields),
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
        _divider(),
        _section("*AI 요약*"),
        _section(summary),
        _context(
            f"분석 기준: {d1_date}  |  데이터 소스: CUR  |  모델: {_BEDROCK_MODEL_ID}"
            f"  |  이상치 기준: μ ± {_ANOMALY_SIGMA}σ"
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
