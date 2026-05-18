"""
monitor_v2/slack/client.py

Bot Token 방식 Slack 클라이언트.

Incoming Webhook과의 차이:
    - Webhook: 단방향 POST, thread_ts 지원 불가
    - Bot Token: chat.postMessage API → thread_ts로 스레드 답글 가능
                 conversations.open → DM 채널 ID 획득 후 DM 발송 가능

환경변수:
    SLACK_BOT_TOKEN:  xoxb-... 형식의 Bot User OAuth Token
    SLACK_CHANNEL_ID: 메시지를 보낼 채널 ID (C로 시작)

사전 조건 (Slack App 설정):
    Bot Token Scopes:
        chat:write           — 채널 메시지 발송
        im:write             — DM 채널 열기
        users:read           — (선택) 유저 정보 조회
    채널에 Bot을 초대해야 chat:write 권한이 작동함
"""

import os
import traceback as _traceback

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from ..utils.blocks import (
    split_by_aggregate as _split_by_aggregate,
    header        as _header,
    section       as _section,
    fields_section as _fields_section,
)

BOT_TOKEN  = os.environ['SLACK_BOT_TOKEN']
CHANNEL_ID = os.environ['SLACK_CHANNEL_ID']

_client = WebClient(token=BOT_TOKEN)


def post_message(text: str, thread_ts: str = None) -> str:
    """
    채널에 텍스트 메시지를 전송한다.

    Args:
        text:      전송할 메시지 텍스트
        thread_ts: 스레드로 달 경우 부모 메시지의 ts. None이면 새 메인 메시지.

    Returns:
        전송된 메시지의 ts 문자열 (스레드 부모로 재사용 가능)
    """
    kwargs = {'channel': CHANNEL_ID, 'text': text}
    if thread_ts:
        kwargs['thread_ts'] = thread_ts

    response = _client.chat_postMessage(**kwargs)
    return response['ts']


def post_blocks(blocks: list, fallback_text: str = '', thread_ts: str = None) -> str:
    """
    Block Kit 블록 배열을 채널에 전송한다.

    Slack은 한 메시지 내 markdown 블록 합산 10,000자를 초과하면 invalid_blocks 에러를 반환한다.
    이를 방지하기 위해 split_by_aggregate()로 블록을 안전한 단위로 분할해 순차 발송한다.
    분할이 일어나지 않는 경우 동작은 단일 발송과 동일하다.

    Args:
        blocks:        slack_sdk Block 객체 또는 dict 리스트
        fallback_text: 알림 미리보기에 표시될 텍스트 (blocks 미지원 환경 대비)
        thread_ts:     스레드로 달 경우 부모 메시지의 ts. None이면 새 메인 메시지.

    Returns:
        첫 번째로 전송된 메시지의 ts 문자열 (스레드 부모로 재사용 가능)
    """
    serialized = [b.to_dict() if hasattr(b, 'to_dict') else b for b in blocks]
    batches    = _split_by_aggregate(serialized)

    first_ts = None
    for batch in batches:
        kwargs = {'channel': CHANNEL_ID, 'blocks': batch, 'text': fallback_text}
        if thread_ts:
            kwargs['thread_ts'] = thread_ts
        response = _client.chat_postMessage(**kwargs)
        if first_ts is None:
            first_ts = response['ts']
    return first_ts


def send_dm(slack_user_id: str, text: str) -> None:
    """
    특정 Slack User에게 DM을 발송한다.

    conversations.open으로 IM 채널 ID를 획득한 뒤 chat.postMessage를 호출.
    DM 실패 시 전체 실행을 중단하지 않고 로그만 남긴다.

    Args:
        slack_user_id: Slack User ID (U로 시작, IAM_SLACK_USER_MAP에서 조회)
        text:          DM 내용
    """
    try:
        dm_resp    = _client.conversations_open(users=[slack_user_id])
        dm_channel = dm_resp['channel']['id']
        _client.chat_postMessage(channel=dm_channel, text=text)
    except SlackApiError as e:
        print(f"[DM 발송 실패] user={slack_user_id}, error={e.response['error']}")


def post_error(context: str, error: Exception, meta: dict = None) -> None:
    """
    에러 발생 시 채널에 Block Kit 알림을 전송한다.

    Args:
        context: 에러 발생 단계 식별자 (예: 'cost_collect', 'ec2_report', 'ai_analysis')
        error:   잡힌 예외 객체
        meta:    추가 메타데이터 (report_type / date_mode / account_id / d1_date 등)

    동작:
        - 헤더 + 메타 필드(2열) + 에러 메시지 + traceback 마지막 10줄
        - Block Kit 발송 실패 시 plain text 한 줄로 fallback
        - 그래도 실패하면 조용히 삼켜 Lambda 종료를 막지 않는다.
    """
    error_type = type(error).__name__
    error_msg  = (str(error) or '(메시지 없음)')[:500]

    tb_text  = ''.join(_traceback.format_exception(type(error), error, error.__traceback__))
    tb_tail  = '\n'.join(tb_text.splitlines()[-10:])[:2500]

    fields = [f"*단계*\n`{context}`", f"*에러 타입*\n`{error_type}`"]
    for k, v in (meta or {}).items():
        fields.append(f"*{k}*\n`{v}`")

    blocks = [
        _header("🚨 monitor_v2 오류"),
        _fields_section(fields),
        _section(f"*에러 메시지*\n```{error_msg}```"),
        _section(f"*Traceback (last 10 lines)*\n```{tb_tail}```"),
    ]

    fallback = f"[monitor_v2] {context} 오류: {error_type}: {error_msg[:200]}"
    try:
        post_blocks(blocks, fallback_text=fallback)
    except Exception:
        try:
            post_message(fallback)
        except Exception:
            pass
