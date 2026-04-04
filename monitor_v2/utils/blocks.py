"""
monitor_v2/utils/blocks.py

cost/report.py 와 ec2/report.py 가 공통으로 사용하는
Block Kit 헬퍼, 계산/포맷 헬퍼, 공유 상수를 모아둔 모듈.
"""

from slack_sdk.models.blocks import (
    HeaderBlock, SectionBlock, DividerBlock, ContextBlock,
)
from slack_sdk.models.blocks.basic_components import MarkdownTextObject, PlainTextObject

# ---------------------------------------------------------------------------
# 공유 상수
# ---------------------------------------------------------------------------

SEP = "─" * 60

EC2_SERVICES = {
    'Amazon Elastic Compute Cloud - Compute',
    'Amazon EC2',
    'EC2 - Other',
}

_MD_BLOCK_MAX = 2800  # Slack 3000자 제한에서 여유분 확보


# ---------------------------------------------------------------------------
# Block Kit 헬퍼
# ---------------------------------------------------------------------------

def header(text: str) -> HeaderBlock:
    """굵고 큰 헤더. plain_text 전용, 150자 제한."""
    return HeaderBlock(text=PlainTextObject(text=text[:150]))


def section(text: str) -> SectionBlock:
    """mrkdwn 텍스트 섹션. 소제목 및 본문용."""
    return SectionBlock(text=MarkdownTextObject(text=text))


def divider() -> DividerBlock:
    """섹션 간 구분선."""
    return DividerBlock()


def context(text: str) -> ContextBlock:
    """보조 설명용 작은 mrkdwn 텍스트."""
    return ContextBlock(elements=[MarkdownTextObject(text=text)])


def md_block(text: str) -> dict:
    """Slack 신규 markdown 블록 — Markdown 테이블 렌더링 지원."""
    return {"type": "markdown", "text": text}


def md_table_blocks(headers: list, rows: list) -> list:
    """
    Markdown 테이블을 _MD_BLOCK_MAX 한도에 맞게 분할해 md_block 리스트로 반환.
    분할 시 각 블록마다 헤더 행을 반복 포함한다.

    Args:
        headers: 열 헤더 리스트
        rows:    2차원 리스트 (각 행 = 헤더 길이와 동일한 셀 목록)
    """
    header_text = (
        "| " + " | ".join(str(h) for h in headers) + " |\n"
        + "| " + " | ".join(["---"] * len(headers)) + " |\n"
    )
    chunks, current, cur_len = [], [], len(header_text)
    for row in rows:
        line = "| " + " | ".join(str(c) for c in row) + " |"
        cost = len(line) + 1
        if current and cur_len + cost > _MD_BLOCK_MAX:
            chunks.append(current)
            current, cur_len = [], len(header_text)
        current.append(line)
        cur_len += cost
    if current:
        chunks.append(current)
    return [md_block(header_text + "\n".join(c)) for c in chunks] if chunks \
           else [md_block(header_text.rstrip())]


def table_section(title: str, headers: list, rows: list) -> list:
    """소제목 section + 전체 행 Markdown 테이블 블록 리스트."""
    return [section(title), *md_table_blocks(headers, rows)]


# ---------------------------------------------------------------------------
# 공통 계산 / 포맷 헬퍼
# ---------------------------------------------------------------------------

def calc_change(today: float, compare: float) -> tuple:
    """(delta, pct|None). compare=0이면 pct=None."""
    delta = today - compare
    pct   = (delta / compare * 100.0) if compare else None
    return delta, pct


def fmt_change(delta: float, pct) -> str:
    """+$13.45 (+12.2%) 형식 문자열."""
    sign  = '+' if delta >= 0 else ''
    d_str = f"{sign}${abs(delta):,.2f}" if delta >= 0 else f"-${abs(delta):,.2f}"
    if pct is None:
        suffix = '(신규)' if delta > 0 else '(중단)' if delta < 0 else ''
    else:
        s      = '+' if pct >= 0 else ''
        suffix = f"({s}{pct:.1f}%)"
    return f"{d_str} {suffix}".strip()
