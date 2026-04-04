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

_MD_BLOCK_MAX      = 2800   # Slack 개별 markdown 블록 3000자 제한에서 여유분 확보
_AGGREGATE_MD_MAX  = 9500   # Slack 메시지 내 markdown 블록 합산 10000자 제한에서 여유분 확보


# ---------------------------------------------------------------------------
# Block Kit 헬퍼
# ---------------------------------------------------------------------------

def header(text: str) -> HeaderBlock:
    """굵고 큰 헤더. plain_text 전용, 150자 제한."""
    return HeaderBlock(text=PlainTextObject(text=text[:150]))


def section(text: str) -> SectionBlock:
    """mrkdwn 텍스트 섹션. 소제목 및 본문용."""
    return SectionBlock(text=MarkdownTextObject(text=text))


def fields_section(fields: list) -> SectionBlock:
    """2열 그리드 SectionBlock. fields는 mrkdwn 문자열 리스트 (최대 10개)."""
    return SectionBlock(fields=[MarkdownTextObject(text=f) for f in fields[:10]])


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


def split_by_aggregate(blocks: list) -> list:
    """
    단일 메시지 내 markdown 블록 합산이 _AGGREGATE_MD_MAX를 초과하지 않도록
    blocks를 분할하여 리스트의 리스트로 반환한다.

    markdown 블록(dict, type='markdown')의 text 길이만 합산 대상으로 계산한다.
    분할 경계는 항상 markdown 블록 앞에서 발생하므로 section/divider 등은 이동하지 않는다.
    """
    groups: list  = []
    current: list = []
    agg_len: int  = 0

    for block in blocks:
        if isinstance(block, dict) and block.get('type') == 'markdown':
            text_len = len(block.get('text', ''))
            if current and agg_len + text_len > _AGGREGATE_MD_MAX:
                groups.append(current)
                current, agg_len = [], 0
            agg_len += text_len
        current.append(block)

    if current:
        groups.append(current)

    return groups or [[]]


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
