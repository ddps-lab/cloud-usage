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
    Q11: 리소스 ID별 d1 vs d2 비용 차이.
    예: i-005217980755bcf43, vol-xxxx, arn:aws:s3:::bucket-name

    Returns:
        [{'service': str, 'usage_type': str, 'resource_id': str,
          'cost_d1': float, 'cost_d2': float, 'diff': float}, ...]
    """
    year_d1, month_d1 = _partition(d1_date)
    year_d2, month_d2 = _partition(d2_date)
    months = f"'{month_d1}'" if month_d1 == month_d2 else f"'{month_d1}', '{month_d2}'"

    sql = f"""
        SELECT
            product_product_name AS service,
            line_item_usage_type AS usage_type,
            line_item_resource_id AS resource_id,
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
        GROUP BY product_product_name, line_item_usage_type, line_item_resource_id
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
            'service':     r['service'],
            'usage_type':  r.get('usage_type', ''),
            'resource_id': r.get('resource_id', ''),
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


def _build_prompt(
    d1_date: date, d2_date: date,
    d1_total: float, d2_total: float,
    service_rows: list, usage_type_rows: list, resource_rows: list,
) -> str:
    diff = d1_total - d2_total
    pct  = (diff / d2_total * 100) if d2_total else 0.0

    def fmt_rows(rows, key_fields: list) -> str:
        lines = []
        for r in rows:
            keys = ' / '.join(str(r[k]) for k in key_fields if r.get(k))
            lines.append(f"  - {keys}: {_fmt_sign(r['diff'])} (어제 ${r['cost_d1']:,.2f} / 그제 ${r['cost_d2']:,.2f})")
        return '\n'.join(lines) if lines else '  - (데이터 없음)'

    svc_text   = fmt_rows(service_rows,    ['service'])
    type_text  = fmt_rows(usage_type_rows, ['service', 'usage_type'])
    rsrc_text  = fmt_rows(resource_rows,   ['service', 'usage_type', 'resource_id'])

    return f"""어제({d1_date}) AWS 비용: ${d1_total:,.2f}
그제({d2_date}) AWS 비용: ${d2_total:,.2f}
변화: {_fmt_sign(diff)} ({pct:+.1f}%)

[서비스별 변화 (상위 {len(service_rows)}개, 변동 큰 순)]
{svc_text}

[리소스 타입별 변화 (상위 {len(usage_type_rows)}개)]
{type_text}

[리소스별 변화 (상위 {len(resource_rows)}개)]
{rsrc_text}

위 데이터를 바탕으로 어제 AWS 비용 변화를 요약하세요.

형식 예시 (이 텍스트는 출력하지 말 것):

전일 대비 $X.XX (±X%) 증가했으며, 주요 원인은 EC2와 S3입니다.

▲ 증가 원인
EC2 — $22.01, c8gd.48xlarge Spot 인스턴스 사용량 급증
S3 — $8.93, 미국(us-west-2) → 서울(ap-northeast-2) 데이터 전송 비용

▼ 감소 원인
Bedrock — $0.64, Nova/GPT 모델 입력 토큰 사용 감소

[작성 규칙]
- 첫 문장: "전일 대비 $금액 (±X%) 증가/감소했으며, 주요 원인은 [서비스 2~3개]입니다." 한 문장으로 결론 먼저.
- 소제목 "▲ 증가 원인" / "▼ 감소 원인"은 반드시 단독 줄로 작성. 앞에 공백이나 들여쓰기 없이.
- 소제목 바로 다음 줄부터 항목 나열. 항목 형식: "서비스명 — $금액, 한 줄 설명"
- 설명은 usage_type 값을 그대로 쓰지 말고, 사람이 읽을 수 있는 자연어로 해석할 것.
  예) USW2-SpotUsage:c8gd.48xlarge → c8gd.48xlarge Spot 인스턴스 사용량
      APN2-USW2-AWS-Out-Bytes → 미국→서울 데이터 전송
- 서비스명 단축형: Amazon EC2 → EC2, Amazon S3 → S3, AWS Lambda → Lambda
- 동일 서비스의 중복 항목은 합산하여 한 줄로 통합.
- ** * # 등 마크다운 기호 사용 금지.
- 증가/감소 항목이 없으면 해당 소제목 전체 생략.
- 한국어로 작성."""


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
