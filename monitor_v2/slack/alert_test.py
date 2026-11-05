"""
monitor_v2/slack/alert_test.py

system-monitoring 알림이 무엇을 지켜야 하는가.

  1. ALERT_CHANNEL_ID 가 없으면 아무것도 보내지 않는다 (로컬, pytest).
  2. 한 invocation 에서 같은 에러는 한 번만 보내고, 다음 invocation 에는 다시 보낸다.
  3. 메시지에 Function / Request ID / Status Reason / Error Code 와 두 링크 버튼이 있다.
     botocore ClientError 는 Code 와 Message 를 그대로 보인다.
  4. 링크는 그 invocation 의 log stream 으로 간다 (콘솔 인코딩 '$252F' 등).
  5. CE forecast 가 실패하면 알림이 나가고, 리포트 계산은 그대로 추정으로 간다.
  6. Slack 발송이 실패해도 예외를 올리지 않는다.

Slack 은 부르지 않는다. client._client 를 기록만 하는 가짜로 바꾼다.

실행: python3 -m pytest monitor_v2/slack/alert_test.py
"""
import os
from datetime import date
from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError

os.environ.setdefault('SLACK_BOT_TOKEN', 'not-a-real-token')
os.environ.setdefault('SLACK_CHANNEL_ID', 'C000000000')

from monitor_v2.slack import alert, client  # noqa: E402
from monitor_v2.cost import data  # noqa: E402

CTX = SimpleNamespace(
    function_name='monitor-v2-daily-report',
    aws_request_id='req-123',
    log_group_name='/aws/lambda/monitor-v2-daily-report',
    log_stream_name='2026/11/05/[$LATEST]abc',
)

INSUFFICIENT = ClientError(
    {'Error': {'Code': 'DataUnavailableException',
               'Message': 'Insufficient amount of historical data'}},
    'GetCostForecast')


class FakeSlack:
    def __init__(self, fail=False):
        self.calls, self.fail = [], fail

    def chat_postMessage(self, **kw):
        if self.fail:
            raise RuntimeError('slack down')
        self.calls.append(kw)
        return {'ts': '1'}


@pytest.fixture
def slack(monkeypatch):
    fake = FakeSlack()
    monkeypatch.setattr(client, '_client', fake)
    monkeypatch.setenv('ALERT_CHANNEL_ID', 'CALERT')
    monkeypatch.setenv('AWS_REGION', 'ap-northeast-2')
    alert.set_invocation(CTX, {'report_type': 'all'})
    return fake


def _text(blocks):
    return str(blocks)


def test_no_channel_sends_nothing(monkeypatch):
    fake = FakeSlack()
    monkeypatch.setattr(client, '_client', fake)
    monkeypatch.delenv('ALERT_CHANNEL_ID', raising=False)
    alert.set_invocation(CTX)
    assert alert.notify('x', INSUFFICIENT) is False
    assert fake.calls == []


def test_same_error_once_per_invocation(slack):
    assert alert.notify('Cost Explorer GetCostForecast', INSUFFICIENT) is True
    assert alert.notify('Cost Explorer GetCostForecast', INSUFFICIENT) is False
    alert.set_invocation(CTX)
    assert alert.notify('Cost Explorer GetCostForecast', INSUFFICIENT) is True
    assert len(slack.calls) == 2


def test_message_fields_and_buttons(slack):
    alert.notify('Cost Explorer GetCostForecast', INSUFFICIENT,
                 handling='이달 예상 비용을 산정했습니다.', meta={'date': '2026-11-05'})
    call = slack.calls[0]
    assert call['channel'] == 'CALERT'
    text = _text(call['blocks'])
    for s in ('`monitor-v2-daily-report`', '`req-123`', 'Cost Explorer GetCostForecast',
              '*Status:* HANDLED', 'Insufficient amount of historical data',
              '*Error Code:* DataUnavailableException', '*Handling:* 이달 예상 비용을 산정했습니다.',
              '*Report:* Main 1·2 일일 비용 요약 및 EC2 상세 (기준일 2026-11-05)'):
        assert s in text
    buttons = call['blocks'][-1]['elements']
    assert [b['text']['text'] for b in buttons] == ['View Lambda Details', 'View CloudWatch Logs']
    assert buttons[0]['url'].endswith('#/functions/monitor-v2-daily-report?tab=monitoring')
    assert ('log-group/$252Faws$252Flambda$252Fmonitor-v2-daily-report'
            '/log-events/2026$252F11$252F05$252F$255B$2524LATEST$255Dabc') in buttons[1]['url']


def test_local_run_links_log_group():
    alert.set_invocation(None)
    blocks = alert.build_blocks('x', RuntimeError('boom'), 'FAILED')
    url = blocks[-1]['elements'][1]['url']
    assert url.endswith('log-group/$252Faws$252Flambda$252Fmonitor-v2-daily-report')
    assert blocks[0]['text']['text'].startswith('🚨 Daily Report Failed')


def test_forecast_failure_alerts_and_estimates(slack):
    class FailingCE:
        def get_cost_forecast(self, **kw):
            raise INSUFFICIENT
    forecast, source = data.fetch_forecast_or_estimate(FailingCE(), 81.24, date(2026, 11, 4))
    assert source == 'ESTIMATE' and forecast == pytest.approx(528.06)
    assert len(slack.calls) == 1
    assert 'Insufficient amount of historical data' in _text(slack.calls[0]['blocks'])


def test_post_error_also_alerts_failed(slack):
    # 리포트 채널 발송도 같은 가짜 _client 로 간다. 알림 채널로 간 것만 본다.
    client.post_error('cost_collect', RuntimeError('athena down'), {'report_type': 'all'})
    alert_calls = [c for c in slack.calls if c['channel'] == 'CALERT']
    assert len(alert_calls) == 1
    text = _text(alert_calls[0]['blocks'])
    assert '*Status:* FAILED' in text
    assert '*Failure Type:* 비용 데이터 수집 단계 (`cost_collect`)' in text
    assert alert_calls[0]['blocks'][1]['text']['text'].endswith(
        '*Report:* Main 1·2 일일 비용 요약 및 EC2 상세')


def test_slack_failure_is_swallowed(monkeypatch):
    monkeypatch.setattr(client, '_client', FakeSlack(fail=True))
    monkeypatch.setenv('ALERT_CHANNEL_ID', 'CALERT')
    alert.set_invocation(CTX)
    assert alert.notify('x', INSUFFICIENT) is False
