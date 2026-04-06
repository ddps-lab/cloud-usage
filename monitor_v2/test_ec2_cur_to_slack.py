"""
monitor_v2/test_ec2_cur_to_slack.py

Athena CUR 기반 EC2 리포트 Slack 발송 테스트.

test_ec2_to_slack.py 와 동일한 메시지 구성으로 발송하되
비용 데이터 소스를 Cost Explorer API → Athena CUR 쿼리로 교체한다.

실행:
    python -m monitor_v2.test_ec2_cur_to_slack
    또는
    python monitor_v2/test_ec2_cur_to_slack.py

필요 환경변수 (.env):
    SLACK_BOT_TOKEN
    SLACK_CHANNEL_ID
    ATHENA_OUTPUT_LOCATION
    ATHENA_DATABASE          (선택, 기본: hyu_ddps_logs)
    ATHENA_WORKGROUP         (선택, 기본: primary)
    ACCOUNT_NAME             (선택, 기본: hyu-ddps)
"""

import sys
import boto3
from pathlib import Path
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(Path(__file__).parent.parent))

from print_test.utils.environment import setup_environment
setup_environment()

from monitor_v2.cost.data_cur import collect_all as collect_cost_data_cur
from monitor_v2.ec2.data_cur  import collect_all as collect_ec2_data_cur
from monitor_v2.ec2.report_cur import send_ec2_cur_report

KST = timezone(timedelta(hours=9))

if __name__ == "__main__":
    today_kst = datetime.now(KST).date()

    sts        = boto3.client('sts')
    account_id = sts.get_caller_identity()['Account']

    ec2_client  = boto3.client('ec2', region_name='us-east-1')
    ec2_regions = [
        r['RegionName']
        for r in ec2_client.describe_regions(
            Filters=[{'Name': 'opt-in-status', 'Values': ['opt-in-not-required', 'opted-in']}]
        )['Regions']
    ]

    cost_data = collect_cost_data_cur(today_kst)
    d1_date   = cost_data['d1_date']

    ec2_data = collect_ec2_data_cur(ec2_regions, account_id, d1_date)
    send_ec2_cur_report(cost_data, ec2_data)
