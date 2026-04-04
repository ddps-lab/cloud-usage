"""
monitor_v2/lambda_handler.py

Lambda 진입점.

────────────────────────────────────────────────────────────────────
테스트 이벤트 (Lambda 콘솔 Test 버튼 또는 aws lambda invoke)
────────────────────────────────────────────────────────────────────
  {"source": "test"}

  → Slack에 [TEST] 안내 메시지를 추가해 실서비스 리포트와 구분.
    실제 API를 호출하므로 비용이 발생하며 실 채널에 메시지가 발송됨.

────────────────────────────────────────────────────────────────────
운영 이벤트 (EventBridge cron → 22:05 KST)
────────────────────────────────────────────────────────────────────
  cron(5 13 * * ? *)    ← UTC 13:05 = KST 22:05

  EventBridge Rule 설정:
      aws events put-rule \
        --name monitor-v2-daily \
        --schedule-expression "cron(5 13 * * ? *)" \
        --state ENABLED

  Lambda 타겟 추가:
      aws events put-targets \
        --rule monitor-v2-daily \
        --targets "Id=monitor-v2,Arn=<LAMBDA_ARN>"

────────────────────────────────────────────────────────────────────
필수 환경변수
────────────────────────────────────────────────────────────────────
  SLACK_BOT_TOKEN     xoxb-... (Bot User OAuth Token)
  SLACK_CHANNEL_ID    C...     (대상 채널 ID)
  ACCOUNT_NAME        hyu-ddps (리포트 헤더 표시용)

선택 환경변수
────────────────────────────────────────────────────────────────────
  IAM_SLACK_USER_MAP  '{"kim": "U01234567", ...}'
                      IAM username → Slack User ID 매핑 (DM 발송용)

────────────────────────────────────────────────────────────────────
Lambda IAM Role 권한
────────────────────────────────────────────────────────────────────
  ce:GetCostAndUsage
  ec2:DescribeInstances, DescribeVolumes, DescribeSnapshots, DescribeImages
  cloudtrail:LookupEvents
  sts:GetCallerIdentity
"""

import boto3
from datetime import datetime, timedelta, timezone

from .cost.data import collect_all as collect_cost_data
from .ec2.data import collect_all as collect_ec2_data
from .cost.report import send_main1_report
from .ec2.report import send_main2_report
from .slack import client as slack

KST = timezone(timedelta(hours=9))


def lambda_handler(event, context):
    """
    Lambda 핸들러.

    Args:
        event:   {'source': 'test'} 또는 EventBridge 이벤트 dict
        context: Lambda context 객체

    Returns:
        200 (성공) / 500 (실패)
    """
    is_test = (event or {}).get('source') == 'test'

    try:
        today_kst = datetime.now(KST).date()
        session = boto3.session.Session()
        ce = boto3.client('ce', region_name='us-east-1')
        sts = boto3.client('sts')

        account_id = sts.get_caller_identity()['Account']
        ec2_client = session.client('ec2', region_name='us-east-1')
        response = ec2_client.describe_regions(
            Filters=[{
                'Name': 'opt-in-status',
                'Values': ['opt-in-not-required', 'opted-in']
            }]
        )
        ec2_regions = [r['RegionName'] for r in response['Regions']]

        # ── 데이터 수집 ──────────────────────────────────────────────────
        cost_data = collect_cost_data(today_kst)

        d1_date = cost_data['d1_date']
        period_d1 = {
            'Start': d1_date.strftime('%Y-%m-%d'),
            'End':   (d1_date + timedelta(days=1)).strftime('%Y-%m-%d'),
        }

        ec2_data = collect_ec2_data(ec2_regions, account_id, ce, period_d1)

        # ── Slack 발송 ───────────────────────────────────────────────────
        if is_test:
            slack.post_message("[TEST] monitor_v2 리포트 발송 시작")

        send_main1_report(cost_data)
        send_main2_report(cost_data, ec2_data)

        if is_test:
            slack.post_message("[TEST] 리포트 발송 완료")

        return 200

    except Exception as e:
        import traceback
        slack.post_error(context="lambda_handler", error=e)
        print(traceback.format_exc())
        return 500
