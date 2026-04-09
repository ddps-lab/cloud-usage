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
            })

    return sorted(service_map.values(), key=lambda x: abs(x['total_diff']), reverse=True)


def _build_prompt(
    d1_date: date, d2_date: date,
    d1_total: float, d2_total: float,
    service_rows: list, usage_type_rows: list, resource_rows: list,
) -> str:
    diff = d1_total - d2_total
    pct  = (diff / d2_total * 100) if d2_total else 0.0

    merged = _merge_rows(service_rows, resource_rows)

    def fmt_merged(rows: list) -> str:
        blocks = []
        for svc in rows:
            label = _CHANGE_LABEL[svc['change_type']]
            header = (
                f"{svc['service']}: [{label}] {_fmt_sign(svc['total_diff'])}"
                f" (어제 ${svc['cost_d1']:,.2f} / 그제 ${svc['cost_d2']:,.2f})"
            )
            detail_lines = []
            for d in svc['details']:
                who = f"생성자: {d['iam_user']}" if d.get('iam_user') else '생성자: 없음'
                detail_lines.append(f"    usage={d['usage_human']}  {who}")
            blocks.append(header + ('\n' + '\n'.join(detail_lines) if detail_lines else ''))
        return '\n'.join(blocks) if blocks else '  (데이터 없음)'

    merged_text = fmt_merged(merged)

    return f"""어제({d1_date}) AWS 비용: ${d1_total:,.2f}
그제({d2_date}) AWS 비용: ${d2_total:,.2f}
변화: {_fmt_sign(diff)} ({pct:+.1f}%)

[서비스별 비용 변화 — 변동 큰 순]
각 항목에 [어제 처음 발생] / [어제 사용 없음] / [증감] 레이블이 붙어 있습니다.
usage= 행의 생성자가 그 리소스를 만든 IAM User입니다.

{merged_text}

위 데이터를 바탕으로 어제 AWS 비용 변화를 한국어로 요약하세요.

=== 출력 형식 ===

첫 줄: "전일 대비 $금액 (±X%) 증가/감소했으며, 주요 원인은 [서비스 2~3개]입니다."
빈 줄
▲ 증가 원인   ← [어제 처음 발생] 또는 [증감]에서 비용이 늘어난 항목. 없으면 소제목 생략.
서비스명 — $금액  설명
▼ 감소 원인   ← [어제 사용 없음] 또는 [증감]에서 비용이 줄어든 항목. 없으면 소제목 생략.
서비스명 — $금액  설명

=== 출력 예시 (이 텍스트는 출력하지 말 것) ===

입력 예시:
Amazon EC2: [어제 처음 발생] +$28.50 (어제 $28.50 / 그제 $0.00)
    usage=미국 서부(us-west-2) c8gd.48xlarge Spot 인스턴스  생성자: mhsong
Amazon S3: [증가] +$5.10 (어제 $8.20 / 그제 $3.10)
    usage=미국 서부(us-west-2)에서 서울(ap-northeast-2)로 나가는 데이터 전송  생성자: jhpark
Amazon Bedrock: [어제 사용 없음] -$0.80 (어제 $0.00 / 그제 $0.80)
    usage=미국 동부(us-east-1) Bedrock 모델 호출  생성자: mhsong
Amazon Virtual Private Cloud: [어제 사용 없음] -$0.30 (어제 $0.00 / 그제 $0.30)
    usage=미국 서부(us-west-2) VPC 엔드포인트 사용 시간  생성자: mhsong
AWS Lambda: [어제 사용 없음] -$0.20 (어제 $0.00 / 그제 $0.20)
    usage=미국 동부(us-east-1) Lambda 함수 실행  생성자: jhpark
AWS Cost Explorer: [어제 사용 없음] -$0.10 (어제 $0.00 / 그제 $0.10)
    usage=Cost Explorer API 조회  생성자: 없음

출력 예시:
전일 대비 $32.40 (+253.1%) 증가했으며, 주요 원인은 EC2와 S3입니다.

▲ 증가 원인
EC2 — $28.50  mhsong이 미국 서부에서 c8gd.48xlarge Spot 인스턴스를 어제 처음 실행함
S3 — $5.10  jhpark의 미국 서부에서 서울로 보내는 데이터 전송량이 전날보다 늘어남

▼ 감소 원인
Bedrock — $0.80  mhsong의 Bedrock 모델 호출이 어제 이루어지지 않음
VPC — $0.30  mhsong의 미국 서부 VPC 엔드포인트 사용이 어제 없었음
Lambda — $0.20  jhpark의 Lambda 함수 실행이 어제 없었음
Cost Explorer — $0.10  Cost Explorer API 조회가 어제 없었음

=== 작성 규칙 ===

레이블 해석:
  [어제 처음 발생] → 그제는 $0, 어제 처음 켜진 것. "처음 시작됨", "어제 처음 실행됨" 등의 표현 사용.
                    "증가"라고 쓰지 말 것.
  [어제 사용 없음] → 어제 $0, 그제에만 사용됨. "어제 없었음", "어제 이루어지지 않음" 등의 표현 사용.
                    "감소"라고 쓰지 말 것.
  [증감]          → 양일 모두 비용 있음. diff 방향에 따라 "증가", "감소" 표현 사용.

금액: 항상 양수. 부호는 ▲/▼ 소제목으로만 구분. 금액 앞에 - 붙이지 말 것.

서비스명 단축:
  Amazon EC2 → EC2 / Amazon S3 → S3 / AWS Lambda → Lambda
  Elastic Load Balancing → ELB / Amazon Virtual Private Cloud → VPC
  Amazon CloudWatch → CloudWatch / AWS Cost Explorer → Cost Explorer
  Amazon CloudFront → CloudFront / Amazon Bedrock → Bedrock

설명 — 모든 항목은 비어있지 않은 설명을 반드시 가져야 한다:
  A. 생성자 있음: "[생성자명]의 [usage 설명]이 [레이블에 맞는 표현]"
     예) mhsong이 미국 서부에서 c8gd.48xlarge Spot 인스턴스를 어제 처음 실행함
     예) mhsong의 Bedrock 모델 호출이 어제 이루어지지 않음
     예) jhpark의 미국 서부에서 서울로 보내는 데이터 전송량이 전날보다 늘어남
  B. 생성자 없음: "[usage 설명]이 [레이블에 맞는 표현]"
     예) Cost Explorer API 조회가 어제 없었음
  C. usage 해석 어려운 경우에도: "[생성자명의] [서비스명] 사용이 [레이블에 맞는 표현]"
     빈칸으로 두는 것은 어떤 경우에도 허용되지 않음.

금지:
  "해당 서비스 사용량 변화" 절대 사용 금지.
  생성자가 있는데 이름을 빠뜨리는 것 금지.
  설명을 빈칸으로 두는 것 금지.
  리소스 ID(i-xxx, vol-xxx, arn:...) 포함 금지.
  ** * # 등 마크다운 기호 사용 금지."""


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
    prompt = _build_prompt(
        d1_date, d2_date, d1_total, d2_total,
        service_rows, usage_type_rows, resource_rows,
    )
    try:
        bedrock = boto3.client('bedrock-runtime', region_name=_BEDROCK_REGION)
        body = json.dumps({
            'messages': [{'role': 'user', 'content': [{'text': prompt}]}],
            'inferenceConfig': {'max_new_tokens': 512},
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
