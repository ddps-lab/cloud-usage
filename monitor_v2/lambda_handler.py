import os
import boto3
from datetime import datetime, timedelta, timezone

from .cost.data_cur import collect_all as collect_cost_data
from .cost.data     import collect_all as collect_cost_data_ce
from .ec2.data_cur  import collect_all as collect_ec2_data
from .cost.report_cur      import send_cur_report
from .ec2.report_cur       import send_ec2_cur_report
from .cost.report_analysis import send_main3_report
from .slack import client as slack

KST = timezone(timedelta(hours=9))

# spotlake 계정은 CUR 미적재 → Cost Explorer(data.py) 경로로 임시 우회
ACCOUNT_NAME = os.environ.get('ACCOUNT_NAME', '')


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

    에러 처리:
        각 단계(init / cost_collect / cost_report / ec2_collect / ec2_report / ai_analysis)를
        개별 try/except로 감싼다. 단계 실패 시 slack.post_error로 알림을 보내고
        had_error 플래그를 세운 뒤 가능한 후속 단계는 계속 진행한다 (부분 실패 허용).
        cost_collect는 EC2 단계의 선행 의존이라 실패 시 EC2 단계를 건너뛴다.

    Returns:
        200 (전 단계 성공) / 500 (한 단계라도 실패)
    """
    event = event or {}
    report_type = event.get('report_type', 'all')
    date_mode   = event.get('date_mode', 'today')

    base_meta = {'report_type': report_type}
    had_error = False

    # ── 날짜 산정 ─────────────────────────────────────────────────
    try:
        today_actual = datetime.now(KST).date()   # 디크리먼트 전 실제 오늘 (spotlake CE 경로용)
        today_kst = today_actual
        if date_mode == 'yesterday':
            today_kst = today_kst - timedelta(days=1)
    except Exception as e:
        slack.post_error(context='init/date', error=e, meta=base_meta)
        return 500

    # ── Main 3: 비용 변화 AI 분석 (08:15 KST, 독립 실행) ────────────
    if report_type == 'analysis':
        try:
            send_main3_report(today_kst)
            return 200
        except Exception as e:
            slack.post_error(
                context='ai_analysis',
                error=e,
                meta={**base_meta, 'date': str(today_kst)},
            )
            return 500

    # ── 공통 초기화: account / regions ──────────────────────────
    try:
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
    except Exception as e:
        slack.post_error(context='init/aws_clients', error=e, meta=base_meta)
        return 500

    base_meta['account_id'] = account_id

    # ── Cost 데이터 수집 ──────────────────────────────────────────
    # spotlake 계정: CUR 미적재 → Cost Explorer(data.py)로 우회.
    #   CE는 24~48h 지연 → collect_all 내부에서 항상 d1 = (인자) - 2.
    #   today_actual을 넘겨 date_mode와 무관하게 항상 D-2 리포트.
    # 그 외 계정: 기존 CUR/Athena 경로 그대로 (forecast만 CE 사용).
    try:
        if ACCOUNT_NAME == 'spotlake':
            cost_data = collect_cost_data_ce(today_actual)
        else:
            cost_data = collect_cost_data(today_kst)
    except Exception as e:
        slack.post_error(context='cost_collect', error=e, meta=base_meta)
        return 500

    base_meta['date'] = str(cost_data.get('d1_date', ''))

    # ── Cost 리포트 발송 (Main 1) ───────────────────────────────
    if report_type in ('cost', 'all'):
        try:
            send_cur_report(cost_data)
        except Exception as e:
            slack.post_error(context='cost_report', error=e, meta=base_meta)
            had_error = True

    # ── EC2 수집 + 리포트 발송 (Main 2) ──────────────────────────
    if report_type in ('ec2', 'all'):
        ec2_data = None
        try:
            ec2_data = collect_ec2_data(ec2_regions, account_id, cost_data['d1_date'])
        except Exception as e:
            slack.post_error(context='ec2_collect', error=e, meta=base_meta)
            had_error = True

        if ec2_data is not None:
            try:
                send_ec2_cur_report(cost_data, ec2_data)
            except Exception as e:
                slack.post_error(context='ec2_report', error=e, meta=base_meta)
                had_error = True

    return 500 if had_error else 200
