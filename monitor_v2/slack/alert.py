"""
monitor_v2/slack/alert.py

Daily Report Lambda 에서 난 에러를 운영 알림 채널(system-monitoring)로 보낸다.

리포트 채널로 가는 slack.post_error 는 단계 전체가 실패했을 때만 불린다. 그런데
"CE GetCostForecast 실패 → 자체 추정" 이나 "CUR 조회 실패 → Cost Explorer 대체" 처럼
예외를 잡고 계속 가는 경로는 CloudWatch 로그에만 남아서, 리포트는 나가도 무엇이
대체됐는지 아무도 모른다 (2026-11 "Insufficient amount of historical data").
그래서 두 경우 모두 여기로 보낸다.

    status FAILED  : 단계가 실패해 그 리포트가 나가지 않았다 (post_error 가 부른다)
    status HANDLED : 에러가 났지만 대체 경로로 리포트는 나갔다

환경변수:
    ALERT_CHANNEL_ID: 알림 채널 ID. 없으면 아무것도 보내지 않는다 (로컬 테스트, pytest).
    SLACK_BOT_TOKEN : 리포트와 같은 bot token 을 쓴다.

한 invocation 안에서 같은 (failure_type, 에러) 는 한 번만 보낸다. CUR→CE 대체 경로에서
forecast 를 두 번 부르는 경우가 있어서다. Lambda 는 warm container 에서 모듈 상태를
재사용하므로 set_invocation() 이 매 invocation 시작에 이 기록을 비운다.
"""

import logging
import os
from urllib.parse import quote

log = logging.getLogger(__name__)

_DEFAULT_FUNCTION = 'monitor-v2-daily-report'

_invocation = {}
_sent = set()

# 알림의 Report 줄에 쓰는 리포트 이름 (event.report_type → 표시명)
_REPORT_NAMES = {
    'all':      'Main 1·2 일일 비용 요약 및 EC2 상세',
    'cost':     'Main 1 일일 비용 요약',
    'ec2':      'Main 2 EC2 상세',
    'analysis': 'Main 3 비용 변화 분석',
    'hyperun':  'Main 4 hyperun 작업 비용',
    'vendor':   'Main 5 외부 vendor 청구 금액',
}

# post_error 의 context (lambda_handler 의 단계 이름) → Failure Type 표시명
_STAGE_NAMES = {
    'init/date':        '기준일 산정',
    'init/aws_clients': 'AWS 계정 및 리전 조회',
    'cost_collect':     '비용 데이터 수집',
    'cost_report':      'Main 1 발송',
    'ec2_collect':      'EC2 데이터 수집',
    'ec2_report':       'Main 2 발송',
    'ai_analysis':      'Main 3 분석 및 발송',
    'hyperun_import':   'hyperun 모듈 로드',
    'hyperun_collect':  'hyperun 사용량 수집',
    'hyperun_report':   'Main 4 발송',
    'vendor_import':    'vendor 모듈 로드',
    'vendor_collect':   'vendor 청구 금액 수집',
    'vendor_report':    'Main 5 발송',
}


def stage_label(context: str) -> str:
    """lambda_handler 단계 이름을 '비용 데이터 수집 단계 (cost_collect)' 형태로 바꾼다."""
    name = _STAGE_NAMES.get(context)
    return f"{name} 단계 (`{context}`)" if name else f"`{context}` 단계"


def _report_line(report_type: str, meta: dict) -> str:
    """'Main 1·2 일일 비용 요약 및 EC2 상세 (기준일 2026-11-05)'. 알 수 없으면 빈 문자열."""
    meta = meta or {}
    report_type = meta.get('report_type') or report_type
    name = _REPORT_NAMES.get(report_type, report_type or '')
    day = meta.get('date')
    if name and day:
        return f"{name} (기준일 {day})"
    return name or (f"기준일 {day}" if day else '')


def set_invocation(context=None, event=None) -> None:
    """Lambda context 에서 링크에 쓸 값을 기록한다. lambda_handler 맨 앞에서 부른다."""
    _sent.clear()
    _invocation.clear()
    event = event or {}
    _invocation.update({
        'function_name': getattr(context, 'function_name', None)
                         or os.environ.get('AWS_LAMBDA_FUNCTION_NAME', _DEFAULT_FUNCTION),
        'request_id':    getattr(context, 'aws_request_id', None) or 'local',
        'log_group':     getattr(context, 'log_group_name', None),
        'log_stream':    getattr(context, 'log_stream_name', None),
        'report_type':   event.get('report_type', 'all'),
    })


def _region() -> str:
    return os.environ.get('AWS_REGION') or os.environ.get('AWS_DEFAULT_REGION') or 'ap-northeast-2'


def _console_escape(s: str) -> str:
    """CloudWatch 콘솔 URL fragment 인코딩: URL 인코딩한 뒤 '%' 를 '$25' 로 바꾼다."""
    return quote(s, safe='').replace('%', '$25')


def lambda_url(function_name: str, region: str) -> str:
    return (f"https://{region}.console.aws.amazon.com/lambda/home?region={region}"
            f"#/functions/{function_name}?tab=monitoring")


def logs_url(log_group: str, log_stream: str, region: str) -> str:
    """log stream 이 있으면 그 stream, 없으면(로컬) log group 으로 간다."""
    url = (f"https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}"
           f"#logsV2:log-groups/log-group/{_console_escape(log_group)}")
    if log_stream:
        url += f"/log-events/{_console_escape(log_stream)}"
    return url


def _error_parts(error) -> tuple:
    """(에러 코드, 메시지). botocore ClientError 는 response 의 Code / Message 를 쓴다."""
    resp = getattr(error, 'response', None)
    if isinstance(resp, dict) and resp.get('Error'):
        code = resp['Error'].get('Code') or type(error).__name__
        msg = resp['Error'].get('Message') or str(error)
        return code, msg
    return type(error).__name__, str(error) or '(메시지 없음)'


def build_blocks(failure_type: str, error, status: str, handling: str = None,
                 meta: dict = None) -> list:
    inv = _invocation or {}
    function_name = inv.get('function_name') or _DEFAULT_FUNCTION
    request_id = inv.get('request_id') or 'local'
    region = _region()
    log_group = inv.get('log_group') or f"/aws/lambda/{function_name}"
    account = os.environ.get('ACCOUNT_NAME', 'unknown-account')

    code, msg = _error_parts(error)
    msg = ' '.join(msg.split())[:500]

    lines = [
        f"*Function Name:* `{function_name}`",
        f"*Request ID:* `{request_id}`",
        f"*Failure Type:* {failure_type}",
        f"*Status:* {status}",
        f"*Status Reason:* {msg}",
        f"*Error Code:* {code}",
    ]
    if handling:
        lines.append(f"*Handling:* {handling}")
    report = _report_line(inv.get('report_type'), meta)
    if report:
        lines.append(f"*Report:* {report}")

    title = '🚨 Daily Report Failed' if status == 'FAILED' else '⚠️ Daily Report Error'
    return [
        {'type': 'header', 'text': {'type': 'plain_text', 'emoji': True,
                                    'text': f"{title} | {account}"[:150]}},
        {'type': 'section', 'text': {'type': 'mrkdwn', 'text': '\n'.join(lines)}},
        {'type': 'actions', 'elements': [
            {'type': 'button', 'text': {'type': 'plain_text', 'text': 'View Lambda Details'},
             'url': lambda_url(function_name, region), 'action_id': 'view_lambda'},
            {'type': 'button', 'text': {'type': 'plain_text', 'text': 'View CloudWatch Logs'},
             'url': logs_url(log_group, inv.get('log_stream'), region), 'action_id': 'view_logs'},
        ]},
    ]


def notify(failure_type: str, error, status: str = 'HANDLED', handling: str = None,
           meta: dict = None) -> bool:
    """
    알림 채널로 보낸다. 보냈으면 True.

    ALERT_CHANNEL_ID 가 없거나, 이번 invocation 에 같은 에러를 이미 보냈거나, Slack 발송이
    실패하면 False. 어떤 경우에도 예외를 올리지 않는다 (알림 때문에 리포트가 멈추면 안 된다).
    """
    channel = os.environ.get('ALERT_CHANNEL_ID', '')
    if not channel:
        return False
    key = (failure_type, _error_parts(error))
    if key in _sent:
        return False
    _sent.add(key)

    try:
        from .client import _client
        blocks = build_blocks(failure_type, error, status, handling, meta)
        code, msg = _error_parts(error)
        _client.chat_postMessage(
            channel=channel, blocks=blocks,
            text=f"[{status}] {failure_type}: {code}: {msg[:200]}")
        return True
    except Exception as e:                            # noqa: BLE001
        log.warning("system-monitoring 알림 발송 실패: %s", e)
        return False
