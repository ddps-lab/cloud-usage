"""
monitor_v2/cost/analysis.py

CUR Athena 기반 비용 증감 원인 분석 + Amazon Nova Micro LLM 요약.

3단계 드릴다운:
    Q9   서비스별           (product_product_name)
    Q10  리소스 타입별      (line_item_usage_type)
    Q11  리소스 ID별        (line_item_resource_id)

환경변수:
    BEDROCK_MODEL_ID    기본: amazon.nova-micro-v1:0
    BEDROCK_REGION      기본: us-east-1  (Nova Micro 지원 리전)
"""

import os
import json
from pprint import pprint

import boto3
import logging
from datetime import date, timedelta

from .data_cur import _run_query, _partition

log = logging.getLogger(__name__)

_BEDROCK_MODEL_ID = os.environ.get('BEDROCK_MODEL_ID')
_BEDROCK_REGION   = os.environ.get('BEDROCK_REGION')
_TOP_N            = 10


# ---------------------------------------------------------------------------
# Athena 쿼리 (Q9, Q10, Q11)
# ---------------------------------------------------------------------------

def fetch_service_diff(athena, d1_date: date, d2_date: date) -> list:
    """
    Q9: 서비스별 d1 vs d2 비용 차이.
    변동 절대값이 큰 순으로 TOP_N 반환.

    Returns:
        [{'service': str, 'cost_d1': float, 'cost_d2': float, 'diff': float}, ...]
    """
    year_d1, month_d1 = _partition(d1_date)
    year_d2, month_d2 = _partition(d2_date)
    months = f"'{month_d1}'" if month_d1 == month_d2 else f"'{month_d1}', '{month_d2}'"

    sql = f"""
        SELECT
            product_product_name AS service,
            SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d1_date}')
                     THEN line_item_unblended_cost ELSE 0 END) AS cost_d1,
            SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d2_date}')
                     THEN line_item_unblended_cost ELSE 0 END) AS cost_d2,
            SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d1_date}')
                     THEN line_item_unblended_cost ELSE 0 END)
          - SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d2_date}')
                     THEN line_item_unblended_cost ELSE 0 END) AS diff
        FROM hyu_ddps_logs.cur_logs
        WHERE year  = '{year_d1}'
          AND month IN ({months})
          AND DATE(line_item_usage_start_date) IN (DATE('{d1_date}'), DATE('{d2_date}'))
        GROUP BY product_product_name
        HAVING ABS(
            SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d1_date}')
                     THEN line_item_unblended_cost ELSE 0 END)
          - SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d2_date}')
                     THEN line_item_unblended_cost ELSE 0 END)
        ) > 0.01
        ORDER BY ABS(diff) DESC
        LIMIT {_TOP_N}
    """
    rows = _run_query(athena, sql)
    return [
        {
            'service': r['service'],
            'cost_d1': float(r.get('cost_d1') or 0),
            'cost_d2': float(r.get('cost_d2') or 0),
            'diff':    float(r.get('diff') or 0),
        }
        for r in rows if r.get('service')
    ]


def fetch_usage_type_diff(athena, d1_date: date, d2_date: date) -> list:
    """
    Q10: 리소스 타입별 d1 vs d2 비용 차이.
    예: BoxUsage:t3.medium, USW2-EBS:VolumeUsage.gp3

    Returns:
        [{'service': str, 'usage_type': str, 'cost_d1': float, 'cost_d2': float, 'diff': float}, ...]
    """
    year_d1, month_d1 = _partition(d1_date)
    year_d2, month_d2 = _partition(d2_date)
    months = f"'{month_d1}'" if month_d1 == month_d2 else f"'{month_d1}', '{month_d2}'"

    sql = f"""
        SELECT
            product_product_name AS service,
            line_item_usage_type AS usage_type,
            SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d1_date}')
                     THEN line_item_unblended_cost ELSE 0 END) AS cost_d1,
            SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d2_date}')
                     THEN line_item_unblended_cost ELSE 0 END) AS cost_d2,
            SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d1_date}')
                     THEN line_item_unblended_cost ELSE 0 END)
          - SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d2_date}')
                     THEN line_item_unblended_cost ELSE 0 END) AS diff
        FROM hyu_ddps_logs.cur_logs
        WHERE year  = '{year_d1}'
          AND month IN ({months})
          AND DATE(line_item_usage_start_date) IN (DATE('{d1_date}'), DATE('{d2_date}'))
        GROUP BY product_product_name, line_item_usage_type
        HAVING ABS(
            SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d1_date}')
                     THEN line_item_unblended_cost ELSE 0 END)
          - SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d2_date}')
                     THEN line_item_unblended_cost ELSE 0 END)
        ) > 0.01
        ORDER BY ABS(diff) DESC
        LIMIT {_TOP_N}
    """
    rows = _run_query(athena, sql)
    return [
        {
            'service':    r['service'],
            'usage_type': r.get('usage_type', ''),
            'cost_d1':    float(r.get('cost_d1') or 0),
            'cost_d2':    float(r.get('cost_d2') or 0),
            'diff':       float(r.get('diff') or 0),
        }
        for r in rows if r.get('service')
    ]


def fetch_resource_diff(athena, d1_date: date, d2_date: date) -> list:
    """
    Q11: 리소스 ID별 d1 vs d2 비용 차이 + aws:createdBy 태그(IAM User) 포함.
    예: i-005217980755bcf43 → mhsong이 만든 EC2 인스턴스

    Returns:
        [{'service': str, 'usage_type': str, 'resource_id': str,
          'iam_user': str, 'cost_d1': float, 'cost_d2': float, 'diff': float}, ...]
    """
    year_d1, month_d1 = _partition(d1_date)
    year_d2, month_d2 = _partition(d2_date)
    months = f"'{month_d1}'" if month_d1 == month_d2 else f"'{month_d1}', '{month_d2}'"

    sql = f"""
        SELECT
            product_product_name AS service,
            line_item_usage_type AS usage_type,
            line_item_resource_id AS resource_id,
            COALESCE(resource_tags_aws_created_by, '') AS iam_user,
            SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d1_date}')
                     THEN line_item_unblended_cost ELSE 0 END) AS cost_d1,
            SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d2_date}')
                     THEN line_item_unblended_cost ELSE 0 END) AS cost_d2,
            SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d1_date}')
                     THEN line_item_unblended_cost ELSE 0 END)
          - SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d2_date}')
                     THEN line_item_unblended_cost ELSE 0 END) AS diff
        FROM hyu_ddps_logs.cur_logs
        WHERE year  = '{year_d1}'
          AND month IN ({months})
          AND DATE(line_item_usage_start_date) IN (DATE('{d1_date}'), DATE('{d2_date}'))
          AND line_item_resource_id IS NOT NULL
          AND line_item_resource_id != ''
        GROUP BY product_product_name, line_item_usage_type, line_item_resource_id,
                 COALESCE(resource_tags_aws_created_by, '')
        HAVING ABS(
            SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d1_date}')
                     THEN line_item_unblended_cost ELSE 0 END)
          - SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d2_date}')
                     THEN line_item_unblended_cost ELSE 0 END)
        ) > 0.01
        ORDER BY ABS(diff) DESC
        LIMIT {_TOP_N}
    """

    def _parse_iam_user(raw: str) -> str:
        # "IAMUser:AIDA3OGATNBRMEBUIOEWO:mhsong" → "mhsong"
        parts = raw.split(':')
        return parts[2] if len(parts) >= 3 else raw

    rows = _run_query(athena, sql)
    return [
        {
            'service':     r['service'],
            'usage_type':  r.get('usage_type', ''),
            'resource_id': r.get('resource_id', ''),
            'iam_user':    _parse_iam_user(r['iam_user']) if r.get('iam_user') else '',
            'cost_d1':     float(r.get('cost_d1') or 0),
            'cost_d2':     float(r.get('cost_d2') or 0),
            'diff':        float(r.get('diff') or 0),
        }
        for r in rows if r.get('service')
    ]


# ---------------------------------------------------------------------------
# 프롬프트 구성
# ---------------------------------------------------------------------------

def _fmt_sign(v: float) -> str:
    return f"+${v:,.2f}" if v >= 0 else f"-${abs(v):,.2f}"


_REGION_CODES = {
    'APN1': '도쿄(ap-northeast-1)',
    'APN2': '서울(ap-northeast-2)',
    'APN3': '오사카(ap-northeast-3)',
    'APS1': '싱가포르(ap-southeast-1)',
    'APS2': '시드니(ap-southeast-2)',
    'USE1': '미국 동부(us-east-1)',
    'USE2': '미국 동부(us-east-2)',
    'USW1': '미국 서부(us-west-1)',
    'USW2': '미국 서부(us-west-2)',
    'EUW1': '유럽 서부(eu-west-1)',
    'EUC1': '유럽 중부(eu-central-1)',
    'SAE1': '상파울루(sa-east-1)',
}


def _humanize_usage_type(raw: str) -> str:
    """
    AWS CUR line_item_usage_type 코드를 한국어 자연어로 변환.
    LLM이 raw 코드를 해석하지 않아도 되도록 Python에서 미리 처리.

    예)
      USW2-SpotUsage:c8gd.48xlarge  → 미국 서부(us-west-2) c8gd.48xlarge Spot 인스턴스
      APN2-USW2-AWS-Out-Bytes        → 미국 서부(us-west-2)에서 서울(ap-northeast-2)로 나가는 데이터 전송
      USE1-Bedrock-ModelUnit         → 미국 동부(us-east-1) Bedrock 모델 호출
    """
    if not raw:
        return ''

    parts = raw.split('-')
    regions, remaining = [], []
    for p in parts:
        if p in _REGION_CODES and len(regions) < 2:
            regions.append(p)
        else:
            remaining.append(p)
    rest = '-'.join(remaining)
    region_prefix = _REGION_CODES.get(regions[0], '') if regions else ''

    # 리전간 데이터 전송 (리전 2개 + Out/In/Bytes)
    if len(regions) == 2 and ('Bytes' in rest or 'Out' in rest or 'In' in rest):
        src = _REGION_CODES.get(regions[0], regions[0])
        dst = _REGION_CODES.get(regions[1], regions[1])
        return f"{src}에서 {dst}로 나가는 데이터 전송"

    if 'SpotUsage:' in rest:
        itype = rest.split('SpotUsage:')[-1]
        return f"{region_prefix + ' ' if region_prefix else ''}{itype} Spot 인스턴스"

    if 'BoxUsage:' in rest:
        itype = rest.split('BoxUsage:')[-1]
        return f"{region_prefix + ' ' if region_prefix else ''}{itype} 온디맨드 인스턴스"

    if 'VolumeUsage' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}EBS 볼륨"

    if 'LoadBalancerUsage' in rest or 'LCUUsage' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}로드 밸런서"

    if 'VpcEndpoint' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}VPC 엔드포인트 사용 시간"

    if 'Bedrock' in rest and 'ModelUnit' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}Bedrock 모델 호출"

    if 'Lambda' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}Lambda 함수 실행"

    if 'CloudWatch' in rest or 'Metrics' in rest or 'Logs' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}CloudWatch 모니터링"

    if 'CostExplorer' in rest or 'Cost-Explorer' in rest:
        return 'Cost Explorer API 조회'

    if 'Bytes' in rest or 'DataTransfer' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}데이터 전송"

    return raw  # 해석 불가 시 원본 반환


def _get_change_type(cost_d1: float, cost_d2: float) -> str:
    """
    어제(d1)와 그제(d2) 비용으로 변화 유형 판별.

    Returns:
      'new'     : 그제 $0 → 어제 처음 발생
      'stopped' : 어제 $0 → 그제에만 사용, 어제 중단
      'changed' : 양일 모두 비용 존재, 증감
    """
    if cost_d2 == 0 and cost_d1 > 0:
        return 'new'
    if cost_d1 == 0 and cost_d2 > 0:
        return 'stopped'
    return 'changed'


_CHANGE_LABEL = {
    'new':     '어제 처음 발생',
    'stopped': '어제 사용 없음',
    'changed': '증감',
}


def _aggregate_details(details: list) -> list:
    """
    동일 (usage_human, iam_user) 조합을 하나로 집계.
    resource_id별 중복 행을 제거하고 비용을 합산한다.
    """
    from collections import defaultdict
    agg = defaultdict(lambda: {'diff': 0.0, 'cost_d1': 0.0, 'cost_d2': 0.0, 'count': 0})
    for d in details:
        key = (d['usage_human'], d['iam_user'])
        agg[key]['diff']    += d['diff']
        agg[key]['cost_d1'] += d['cost_d1']
        agg[key]['cost_d2'] += d['cost_d2']
        agg[key]['count']   += 1

    result = []
    for (usage_human, iam_user), v in agg.items():
        result.append({
            'usage_human': usage_human,
            'iam_user':    iam_user,
            'diff':        v['diff'],
            'cost_d1':     v['cost_d1'],
            'cost_d2':     v['cost_d2'],
            'change_type': _get_change_type(v['cost_d1'], v['cost_d2']),
            'count':       v['count'],
        })
    return sorted(result, key=lambda x: abs(x['diff']), reverse=True)


def _merge_rows(service_rows: list, resource_rows: list) -> list:
    """
    Q9(서비스 총합)와 Q11(리소스+IAM User)를 service 기준으로 통합.

    LLM이 섹션 간 cross-reference 추론 없이 한 블록에서 읽을 수 있도록
    Python 단에서 미리 join한다.
    """
    service_map = {
        r['service']: {
            'service':     r['service'],
            'total_diff':  r['diff'],
            'cost_d1':     r['cost_d1'],
            'cost_d2':     r['cost_d2'],
            'change_type': _get_change_type(r['cost_d1'], r['cost_d2']),
            'details':     [],
        }
        for r in service_rows
    }

    for r in resource_rows:
        svc = r['service']
        if svc in service_map:
            service_map[svc]['details'].append({
                'usage_human': _humanize_usage_type(r.get('usage_type', '')),
                'iam_user':    r.get('iam_user', ''),
                'diff':        r['diff'],
                'cost_d1':     r['cost_d1'],
                'cost_d2':     r['cost_d2'],
                'change_type': _get_change_type(r['cost_d1'], r['cost_d2']),
            })

    for svc_data in service_map.values():
        svc_data['details'] = _aggregate_details(svc_data['details'])

    return sorted(service_map.values(), key=lambda x: abs(x['total_diff']), reverse=True)


_SYSTEM_PROMPT = """\
당신은 AWS 비용 분석 요약 도우미입니다.
사용자가 어제/그제 AWS 비용 데이터를 제공하면, 아래 형식과 규칙에 따라 한국어로 요약합니다.

=== 출력 형식 ===

첫 줄: "전일 대비 $금액 (±X%) 증가/감소했으며, 주요 원인은 [서비스 2~3개]입니다."
빈 줄
▲ 증가 원인
서비스명 — $금액  설명
▼ 감소 원인
서비스명 — $금액  설명

증가/감소 어느 쪽이 없으면 해당 소제목(▲/▼) 전체를 생략하세요.
"비용이 증가한 서비스" 섹션의 항목은 반드시 ▲에, "비용이 감소한 서비스" 섹션의 항목은 반드시 ▼에 작성하세요.

=== [타입] 별 표현 규칙 ===

서비스 타입:
  [신규] — 그제 $0이었다가 어제 처음 비용 발생. "처음 사용됨", "어제 처음 발생함" 등으로 서술.
  [중단] — 그제까지 사용하다가 어제 $0. "어제 사용 없었음", "어제 이루어지지 않음" 등으로 서술.
  [증가] — 어제/그제 모두 비용 있고 어제가 더 큼. "늘어남", "더 많이 사용됨" 등으로 서술.
  [감소] — 어제/그제 모두 비용 있고 어제가 더 작음. "줄어듦", "덜 사용됨" 등으로 서술.

usage detail 타입 (서비스 내 세부 항목):
  [신규 발생] — 이 usage가 어제 처음 등장. "처음 켜짐", "처음 시작됨" 등으로 서술.
  [어제 중단] — 이 usage가 어제 사용 없음. "어제 없었음", "중단됨" 등으로 서술.
               절대 "처음"이라는 단어를 쓰지 말 것.
  [증가] / [감소] — 양일 모두 사용. 방향에 맞게 "늘어남" / "줄어듦" 등으로 서술.

=== 설명 작성 규칙 ===

서비스 1줄 설명은 detail 항목들을 종합해 완전한 문장으로 작성하세요.
명사형 종결("~중단됨", "~줄어듦") 대신 동사형 서술("~중단되었습니다", "~줄었습니다")로 마무리하세요.
생성자가 있는 경우 반드시 이름을 포함해 누가 어떤 리소스를 어떻게 했는지 서술하세요.

=== 입출력 예시 (이 텍스트는 출력하지 말 것) ===

입력 예시:
어제(2026-04-09) AWS 비용: $32.69
그제(2026-04-08) AWS 비용: $61.80
전일 대비: -$29.11 (-47.1%)

=== 비용이 증가한 서비스 ===

Bedrock  $0.04  [신규]  어제 $0.04 / 그제 $0.00

=== 비용이 감소한 서비스 ===

EC2  $12.87  [감소]  어제 $30.75 / 그제 $43.62
    mhsong: 미국 서부(us-west-2) c8gd.48xlarge Spot 인스턴스 ×2개  [감소]  어제 $8.19 / 그제 $24.82
    jhpark: 미국 서부(us-west-2) inf2.8xlarge 온디맨드 인스턴스 ×5개  [증가]  어제 $14.85 / 그제 $5.86
    yjjung: 미국 서부(us-west-2) g4dn.12xlarge 온디맨드 인스턴스  [어제 중단]  어제 $0.00 / 그제 $5.34
S3  $12.14  [감소]  어제 $0.36 / 그제 $12.50
    mhsong: 서울(ap-northeast-2)에서 미국 서부(us-west-2)로 나가는 데이터 전송  [감소]  어제 $0.00 / 그제 $8.89
    swjeong: USW2-TimedStorage-ByteHrs  [어제 중단]  어제 $0.00 / 그제 $3.16
Lambda  $3.59  [감소]  어제 $0.93 / 그제 $4.52
ELB  $0.25  [감소]  어제 $0.35 / 그제 $0.60
VPC  $0.13  [감소]  어제 $0.15 / 그제 $0.28
Cost Explorer  $0.10  [감소]  어제 $0.02 / 그제 $0.12
CloudWatch  $0.07  [감소]  어제 $0.11 / 그제 $0.18

출력 예시:
전일 대비 $29.11 (-47.1%) 감소했으며, 주요 원인은 EC2와 S3입니다.

▲ 증가 원인
Bedrock — $0.04  Bedrock 모델 호출이 어제 처음 발생했습니다.

▼ 감소 원인
EC2 — $12.87  mhsong의 c8gd.48xlarge Spot 인스턴스 2대와 yjjung의 g4dn.12xlarge 인스턴스가 각각 사용량 감소 및 중단되었습니다. jhpark의 inf2.8xlarge 인스턴스 5대는 오히려 사용량이 늘었지만 전체적으로 EC2 비용이 줄었습니다.
S3 — $12.14  mhsong의 서울에서 미국 서부로 나가는 데이터 전송이 어제 이루어지지 않았고, swjeong의 스토리지 사용도 어제 없었습니다.
Lambda — $3.59  Lambda 함수 실행 비용이 전날보다 줄었습니다.
ELB — $0.25  로드 밸런서 사용 비용이 전날보다 줄었습니다.
VPC — $0.13  VPC 관련 비용이 전날보다 줄었습니다.
Cost Explorer — $0.10  Cost Explorer API 조회 비용이 전날보다 줄었습니다.
CloudWatch — $0.07  CloudWatch 모니터링 비용이 전날보다 줄었습니다.

=== 금지 사항 ===

설명 빈칸 금지.
리소스 ID(i-xxx, vol-xxx, arn:...) 포함 금지.
금액은 항상 양수. 부호는 ▲/▼ 소제목으로만 구분.
마크다운(** * #) 사용 금지.
"해당 서비스 사용량 변화" 사용 금지.
생성자가 있는데 이름 빠뜨리기 금지."""


# 서비스명 단축 — Python에서 미리 처리해 LLM에 전달
_SVC_SHORT = {
    'Amazon Elastic Compute Cloud': 'EC2',
    'Amazon Simple Storage Service': 'S3',
    'AWS Lambda': 'Lambda',
    'Elastic Load Balancing': 'ELB',
    'Amazon Virtual Private Cloud': 'VPC',
    'AWS Cost Explorer': 'Cost Explorer',
    'AmazonCloudWatch': 'CloudWatch',
    'Amazon CloudFront': 'CloudFront',
    'Amazon Bedrock': 'Bedrock',
}


def _svc_label(svc: dict) -> str:
    if svc['change_type'] == 'new':
        return '신규'
    if svc['change_type'] == 'stopped':
        return '중단'
    return '증가' if svc['total_diff'] > 0 else '감소'


def _detail_label(d: dict) -> str:
    if d['change_type'] == 'new':
        return '신규 발생'
    if d['change_type'] == 'stopped':
        return '어제 중단'
    return '증가' if d['diff'] > 0 else '감소'


def _fmt_section(rows: list) -> str:
    if not rows:
        return '  (없음)'
    blocks = []
    for svc in rows:
        short  = _SVC_SHORT.get(svc['service'], svc['service'])
        amount = abs(svc['total_diff'])
        label  = _svc_label(svc)
        header = (
            f"{short}  ${amount:,.2f}  [{label}]"
            f"  어제 ${svc['cost_d1']:,.2f} / 그제 ${svc['cost_d2']:,.2f}"
        )
        detail_lines = []
        for d in svc['details']:
            who       = d['iam_user'] if d.get('iam_user') else '(생성자 미상)'
            count_str = f" ×{d['count']}개" if d.get('count', 1) > 1 else ""
            dlabel    = _detail_label(d)
            detail_lines.append(
                f"    {who}: {d['usage_human']}{count_str}"
                f"  [{dlabel}]  어제 ${d['cost_d1']:,.2f} / 그제 ${d['cost_d2']:,.2f}"
            )
        blocks.append(header + ('\n' + '\n'.join(detail_lines) if detail_lines else ''))
    return '\n'.join(blocks)


def _build_user_message(
    d1_date: date, d2_date: date,
    d1_total: float, d2_total: float,
    service_rows: list, usage_type_rows: list, resource_rows: list,
) -> str:
    diff = d1_total - d2_total
    pct  = (diff / d2_total * 100) if d2_total else 0.0

    merged = _merge_rows(service_rows, resource_rows)
    #print("merged")
    #pprint(merged)

    increase_rows = [s for s in merged if s['total_diff'] > 0]
    decrease_rows = [s for s in merged if s['total_diff'] < 0]

    increase_text = _fmt_section(increase_rows)
    decrease_text = _fmt_section(decrease_rows)

    #print("increase_text")
    #pprint(increase_text)
    #print("decrease_text")
    #pprint(decrease_text)

    return f"""어제({d1_date}) AWS 비용: ${d1_total:,.2f}
그제({d2_date}) AWS 비용: ${d2_total:,.2f}
전일 대비: {_fmt_sign(diff)} ({pct:+.1f}%)

=== 비용이 증가한 서비스 ===

{increase_text}

=== 비용이 감소한 서비스 ===

{decrease_text}

위 데이터를 요약하세요."""


# ---------------------------------------------------------------------------
# Bedrock Nova Micro 호출
# ---------------------------------------------------------------------------

def summarize(
    d1_date: date, d2_date: date,
    d1_total: float, d2_total: float,
    service_rows: list, usage_type_rows: list, resource_rows: list,
) -> str:
    """
    Nova Micro에게 비용 증감 원인 분석을 요청하고 요약 텍스트를 반환한다.

    실패 시 폴백 텍스트 반환 (Lambda 전체 실패 방지).
    """
    user_message = _build_user_message(
        d1_date, d2_date, d1_total, d2_total,
        service_rows, usage_type_rows, resource_rows,
    )
    #print("user_message")
    #pprint(user_message)
    try:
        bedrock = boto3.client('bedrock-runtime', region_name=_BEDROCK_REGION)
        body = json.dumps({
            'system':   [{'text': _SYSTEM_PROMPT}],
            'messages': [{'role': 'user', 'content': [{'text': user_message}]}],
            'inferenceConfig': {
                'max_new_tokens': 512,
                'temperature': 0,
            },
        })
        resp   = bedrock.invoke_model(
            modelId=_BEDROCK_MODEL_ID,
            body=body,
            contentType='application/json',
            accept='application/json',
        )
        result = json.loads(resp['body'].read())
        return result['output']['message']['content'][0]['text'].strip()

    except Exception as e:
        log.error("Bedrock 호출 실패: %s", e)
        diff = d1_total - d2_total
        direction = "증가" if diff >= 0 else "감소"
        return f"LLM 분석 실패 (Bedrock 오류). 전일 대비 ${abs(diff):,.2f} {direction}."


# ---------------------------------------------------------------------------
# 일괄 수집 진입점
# ---------------------------------------------------------------------------

def collect_all(d1_date: date) -> dict:
    """
    Athena 3개 쿼리 실행 + Nova Micro 요약 생성.

    Args:
        d1_date: 리포트 기준일 (data_cur.py 와 동일한 d1_date 사용)

    Returns:
        {
            'd1_date':       date,
            'd2_date':       date,
            'd1_total':      float,
            'd2_total':      float,
            'service_rows':  list,
            'usage_type_rows': list,
            'resource_rows': list,
            'summary':       str,   # Nova Micro 요약
        }
    """
    d2_date = d1_date - timedelta(days=1)
    athena  = boto3.client('athena', region_name='ap-northeast-2')

    service_rows    = fetch_service_diff(athena, d1_date, d2_date)
    usage_type_rows = fetch_usage_type_diff(athena, d1_date, d2_date)
    resource_rows   = fetch_resource_diff(athena, d1_date, d2_date)

    d1_total = sum(r['cost_d1'] for r in service_rows)
    d2_total = sum(r['cost_d2'] for r in service_rows)

    summary = summarize(
        d1_date, d2_date, d1_total, d2_total,
        service_rows, usage_type_rows, resource_rows,
    )

    return {
        'd1_date':         d1_date,
        'd2_date':         d2_date,
        'd1_total':        d1_total,
        'd2_total':        d2_total,
        'service_rows':    service_rows,
        'usage_type_rows': usage_type_rows,
        'resource_rows':   resource_rows,
        'summary':         summary,
    }
