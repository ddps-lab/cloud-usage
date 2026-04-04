import sys
import os
import boto3
from pathlib import Path
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(Path(__file__).parent.parent))

from print_test.utils.environment import setup_environment
setup_environment()

from monitor_v2.cost.data import collect_all as collect_cost_data
from monitor_v2.ec2.data  import collect_all as collect_ec2_data
from monitor_v2.ec2.report import send_main2_report

KST = timezone(timedelta(hours=9))

if __name__ == "__main__":
    today_kst = datetime.now(KST).date()
    session   = boto3.session.Session()
    ce        = boto3.client('ce', region_name='us-east-1')
    sts       = boto3.client('sts')

    account_id = sts.get_caller_identity()['Account']

    ec2_client = session.client('ec2', region_name='us-east-1')
    response   = ec2_client.describe_regions(
        Filters=[{'Name': 'opt-in-status', 'Values': ['opt-in-not-required', 'opted-in']}]
    )
    ec2_regions = [r['RegionName'] for r in response['Regions']]

    cost_data = collect_cost_data(today_kst)

    d1_date   = cost_data['d1_date']
    period_d1 = {
        'Start': d1_date.strftime('%Y-%m-%d'),
        'End':   (d1_date + timedelta(days=1)).strftime('%Y-%m-%d'),
    }

    ec2_data = collect_ec2_data(ec2_regions, account_id, ce, period_d1)
    send_main2_report(cost_data, ec2_data)
