"""
monitor_v2/test_cost_cur_to_slack.py

Athena CUR 기반 Cost 리포트 Slack 발송 테스트.

test_cost_to_slack.py 와 동일한 메시지 구성으로 발송하되
데이터 소스를 Cost Explorer API → Athena CUR 쿼리로 교체한다.

실행:
    python -m monitor_v2.test_cost_cur_to_slack
    또는
    python monitor_v2/test_cost_cur_to_slack.py

필요 환경변수 (.env):
    SLACK_BOT_TOKEN
    SLACK_CHANNEL_ID
    ATHENA_OUTPUT_LOCATION   예: s3://my-bucket/athena-results/
    ATHENA_DATABASE
    ATHENA_WORKGROUP
    ACCOUNT_NAME
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(Path(__file__).parent.parent))

from print_test.utils.environment import setup_environment
setup_environment()

from monitor_v2.cost.data_cur import collect_all as collect_cost_data_cur
from monitor_v2.cost.report_cur import send_cur_report

KST = timezone(timedelta(hours=9))

if __name__ == "__main__":
    today_kst = datetime.now(KST).date()
    cost_data = collect_cost_data_cur(today_kst)
    send_cur_report(cost_data)
