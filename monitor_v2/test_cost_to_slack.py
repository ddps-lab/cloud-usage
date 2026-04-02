import sys
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
import boto3

sys.path.insert(0, str(Path(__file__).parent.parent))

# setup_environment()를 먼저 호출해 환경변수를 세팅한 뒤
# slack/client.py 처럼 모듈 레벨에서 os.environ을 읽는 모듈을 임포트한다.
from print_test.utils.environment import setup_environment
setup_environment()

from monitor_v2.cost.data import collect_all as collect_cost_data
from monitor_v2.cost.report import send_main1_report

KST = timezone(timedelta(hours=9))

if __name__ == "__main__":
    today_kst = datetime.now(KST).date() - timedelta(days=2)
    session = boto3.session.Session()
    ce = boto3.client('ce', region_name='us-east-1')
    sts = boto3.client('sts')

    cost_data = collect_cost_data(today_kst)
    send_main1_report(cost_data)
