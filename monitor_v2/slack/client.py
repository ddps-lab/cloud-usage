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

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

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

    Args:
        blocks:        slack_sdk Block 객체 또는 dict 리스트
        fallback_text: 알림 미리보기에 표시될 텍스트 (blocks 미지원 환경 대비)
        thread_ts:     스레드로 달 경우 부모 메시지의 ts. None이면 새 메인 메시지.

    Returns:
        전송된 메시지의 ts 문자열
    """
    serialized = [b.to_dict() if hasattr(b, 'to_dict') else b for b in blocks]
    kwargs = {'channel': CHANNEL_ID, 'blocks': serialized, 'text': fallback_text}
    if thread_ts:
        kwargs['thread_ts'] = thread_ts

    response = _client.chat_postMessage(**kwargs)
    return response['ts']


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


def post_error(context: str, error: Exception) -> None:
    """
    에러 발생 시 채널에 알림을 전송한다.
    전송 자체가 실패해도 예외를 삼켜 Lambda 종료를 막지 않는다.
    """
    msg = f"[monitor_v2] 오류 발생\n컨텍스트: {context}\n오류: {str(error)}"
    try:
        post_message(msg)
    except Exception:
        pass
