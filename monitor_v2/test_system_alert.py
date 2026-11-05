"""
monitor_v2/test_system_alert.py

system-monitoring 알림(slack/alert.py)을 Slack 채널에서 눈으로 확인하는 로컬 스크립트.
Lambda 는 부르지 않는다.

  forecast: 진짜 Cost Explorer GetCostForecast 를 불러서 난 에러를 그대로 알린다 (HANDLED).
  failed  : 단계 실패(post_error)를 흉내 내 FAILED 알림을 보낸다. 리포트 채널 알림도 같이 간다.

링크 버튼이 진짜로 열리도록, 배포된 Lambda 의 가장 최근 log stream 을 context 로 넣는다.

⚠️ 알림 채널과 리포트 채널 모두 --channel(기본: 테스트 채널 C0ANVAUMFL5)로 보낸다.

실행:
    uv run python -m monitor_v2.test_system_alert
    uv run python -m monitor_v2.test_system_alert --scenario failed
    uv run python -m monitor_v2.test_system_alert --scenario all
"""

import argparse
import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from print_test.utils.environment import setup_environment

TEST_CHANNEL = 'C0ANVAUMFL5'
FUNCTION_NAME = 'monitor-v2-daily-report'
LOG_GROUP = f'/aws/lambda/{FUNCTION_NAME}'


def _latest_context():
    import boto3
    logs = boto3.client('logs', region_name='ap-northeast-2')
    streams = logs.describe_log_streams(
        logGroupName=LOG_GROUP, orderBy='LastEventTime', descending=True, limit=1,
    )['logStreams']
    return SimpleNamespace(
        function_name=FUNCTION_NAME,
        aws_request_id='local-test',
        log_group_name=LOG_GROUP,
        log_stream_name=streams[0]['logStreamName'] if streams else None,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--scenario', choices=['forecast', 'failed', 'all'], default='forecast')
    parser.add_argument('--channel', default=TEST_CHANNEL)
    args = parser.parse_args()

    setup_environment()
    os.environ['SLACK_CHANNEL_ID'] = args.channel
    os.environ['ALERT_CHANNEL_ID'] = args.channel
    os.environ.setdefault('AWS_REGION', 'ap-northeast-2')

    import boto3
    from monitor_v2.cost import data
    from monitor_v2.slack import alert, client

    alert.set_invocation(_latest_context(), {'report_type': 'all'})
    today = datetime.now(timezone(timedelta(hours=9))).date()

    if args.scenario in ('forecast', 'all'):
        ce = boto3.client('ce', region_name='us-east-1')
        forecast, source = data.fetch_forecast_or_estimate(ce, 100.0, today)
        print(f"forecast={forecast:.2f} source={source}")

    if args.scenario in ('failed', 'all'):
        try:
            raise TimeoutError('Athena 쿼리 타임아웃 (300s): exec_id=local-test')
        except TimeoutError as e:
            client.post_error('cost_collect', e, {'report_type': 'all', 'date': str(today)})
        print('post_error sent')


if __name__ == '__main__':
    main()
