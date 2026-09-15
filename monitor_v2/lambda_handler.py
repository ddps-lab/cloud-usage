import boto3
import logging
from datetime import datetime, timedelta, timezone

from .cost.data_cur import collect_all as collect_cost_data
from .cost.data     import collect_all as collect_cost_data_ce
from .ec2.data_cur  import collect_all as collect_ec2_data
from .cost.report_cur      import send_cur_report
from .ec2.report_cur       import send_ec2_cur_report
from .cost.report_analysis import send_main3_report
from .slack import client as slack

log = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))


def _has_cost_data(cost_data: dict) -> bool:
    """CUR 조회 결과에 의미있는 비용 데이터가 있는지 판단.

    daily_d1 / daily_d2 가 모두 비어 있고 MTD 총액도 0이면 (해당 월 CUR 미적재 등)
    '데이터 없음'으로 보고 CE fallback 대상이 된다. 예외는 아니지만 빈 결과인
    경우(예: 6월분 CUR 미적재)를 잡아내기 위한 판정.
    """
    if not cost_data:
        return False
    return bool(
        cost_data.get('daily_d1')
        or cost_data.get('daily_d2')
        or (cost_data.get('mtd_this') or 0) > 0
    )


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
        'hyperun'  → Main 4 (hyperun 으로 낸 job 의 team/user/vendor 별 비용)

    ★ 'hyperun' 은 AWS 를 전혀 안 본다. 숫자는 hyperun gateway 의 GET /v1/usage 가
    PacsJob 에서 뽑아 준다. 다른 vendor(RunPod, Shadeform, GCP)의 지출은 AWS CUR
    에 한 줄도 안 남으므로 Main 1 의 Athena 경로로는 애초에 보이지 않는다.

    date_mode 동작:
        'today'     → today_kst = 오늘     → d1_date = 오늘  (KST 22:00, CUR 당일 반영 후)
        'yesterday' → today_kst = 오늘 - 1 → d1_date = 어제  (KST 08:00, CUR 전날까지만)
        ※ CUR·CE 두 경로 모두 today_kst를 받아 동일한 d1_date를 사용 (date_mode 반영).

    에러 처리:
        각 단계(init / cost_collect / cost_report / ec2_collect / ec2_report / ai_analysis)를
        개별 try/except로 감싼다. 단계 실패 시 slack.post_error로 알림을 보내고
        had_error 플래그를 세운 뒤 가능한 후속 단계는 계속 진행한다 (부분 실패 허용).
        cost_collect는 CUR/Athena 경로를 먼저 시도하고, 조회 실패 시 Cost Explorer
        경로로 fallback한다. 두 경로 모두 실패해야 cost_collect 실패로 처리한다.
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
        today_kst = datetime.now(KST).date()
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

    # ── Main 4: hyperun 으로 낸 job (독립 실행) ─────────────────
    #
    # 앞의 셋과 달리 AWS 를 전혀 안 본다. 숫자는 hyperun gateway 가 PacsJob 에서
    # 뽑아 주고 이 함수는 받아서 그린다. 그래서 아래의 sts / region 초기화보다
    # 앞에 있다 — 그 초기화가 실패해도 이 보고서는 나갈 수 있어야 한다.
    #
    # ★ 설정이 없으면 조용히 건너뛴다. HYPERUN_API_BASE 나 HYPERUN_API_TOKEN 이
    # 없는 것은 "아직 안 켰다" 이지 고장이 아니고, 그때마다 에러를 올리면 진짜
    # 고장이 났을 때 아무도 안 본다. 로그에는 남는다.
    if report_type == 'hyperun':
        try:
            from .hyperun.data import collect, UsageUnavailable
            from .hyperun.report import send_main4_report
        except ImportError as e:
            slack.post_error(context='hyperun_import', error=e, meta=base_meta)
            return 500
        try:
            usage = collect()
        except UsageUnavailable as e:
            log.warning('hyperun 사용량을 못 받았다: %s', e)
            return 200
        except Exception as e:
            slack.post_error(context='hyperun_collect', error=e, meta=base_meta)
            return 500
        try:
            send_main4_report(usage, today_kst)
            return 200
        except Exception as e:
            slack.post_error(
                context='hyperun_report',
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
    # 기본: CUR/Athena 경로(data_cur.py). d1 = today_kst (forecast만 CE 사용).
    # 다음 두 경우 모두 Cost Explorer(data.py)로 fallback:
    #   (1) Athena 쿼리 예외 (CUR 테이블 부재 등)
    #   (2) 예외는 없지만 조회 결과가 비어 있음 (해당 월 CUR 미적재 — 6월처럼)
    #   CE 경로도 today_kst를 받아 CUR과 동일한 기준일(d1 = today_kst)을 쓴다.
    #   → date_mode가 그대로 반영됨 (yesterday→어제). ⚠ CE 지연으로 당일/어제는 부분 집계일 수 있음.
    cost_data = None
    try:
        cost_data = collect_cost_data(today_kst)
        if not _has_cost_data(cost_data):
            log.warning("CUR 조회 결과 없음 → Cost Explorer fallback (d1=%s)",
                        cost_data.get('d1_date'))
            cost_data = None
    except Exception as cur_err:
        log.warning("CUR 조회 실패 → Cost Explorer fallback: %s", cur_err)

    if cost_data is None:
        try:
            cost_data = collect_cost_data_ce(today_kst)
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
