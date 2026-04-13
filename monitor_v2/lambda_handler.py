import boto3
from datetime import datetime, timedelta, timezone

from .cost.data_cur import collect_all as collect_cost_data
from .ec2.data_cur  import collect_all as collect_ec2_data
from .cost.report_cur      import send_cur_report
from .ec2.report_cur       import send_ec2_cur_report
from .cost.report_analysis import send_main3_report
from .slack import client as slack

KST = timezone(timedelta(hours=9))


def lambda_handler(event, context):
    """
    Lambda 핸들러.

    Args:
        event: {
            'report_type': 'cost' | 'ec2' | 'all' | 'analysis'  (기본값: 'all'),
            'date_mode':   'today' | 'yesterday'                 (기본값: 'today'),
        }
        context: Lambda context 객체

    report_type:
        'cost'     → Main 1 (비용 요약)
        'ec2'      → Main 2 (EC2 상세)
        'all'      → Main 1 + Main 2  (KST 22:05 트리거)
        'analysis' → Main 3 (비용 변화 AI 분석)  (KST 08:15 트리거)

    date_mode 동작:
        'today'     → today_kst 그대로 → d1_date = today - 1  (KST 22:00, CUR 당일 반영 후)
        'yesterday' → today_kst - 1   → d1_date = today - 2  (KST 08:00, CUR 전날까지만 반영)

    Returns:
        200 (성공) / 500 (실패)
    """
    event = event or {}
    report_type = event.get('report_type', 'all')
    date_mode   = event.get('date_mode', 'today')

    try:
        today_kst = datetime.now(KST).date()
        if date_mode == 'yesterday':
            today_kst = today_kst - timedelta(days=1)

        # ── Main 3: 비용 변화 AI 분석 (08:15 KST, 독립 실행) ────────────
        if report_type == 'analysis':
            from datetime import timedelta as td
            d1_date = today_kst - td(days=1)
            send_main3_report(d1_date)
            return 200

        sts        = boto3.client('sts')
        account_id = sts.get_caller_identity()['Account']

        ec2_client  = boto3.client('ec2', region_name='us-east-1')
        ec2_regions = [
            r['RegionName']
            for r in ec2_client.describe_regions(
                Filters=[{
                    'Name': 'opt-in-status',
                    'Values': ['opt-in-not-required', 'opted-in'],
                }]
            )['Regions']
        ]

        # ── 데이터 수집 (CUR / Athena 기반, forecast만 CE 사용) ──────────
        cost_data = collect_cost_data(today_kst)

        # ── Slack 발송 ───────────────────────────────────────────────────
        if report_type in ('cost', 'all'):
            send_cur_report(cost_data)

        if report_type in ('ec2', 'all'):
            ec2_data = collect_ec2_data(ec2_regions, account_id, cost_data['d1_date'])
            send_ec2_cur_report(cost_data, ec2_data)

        return 200

    except Exception as e:
        import traceback
        slack.post_error(context="lambda_handler", error=e)
        print(traceback.format_exc())
        return 500
