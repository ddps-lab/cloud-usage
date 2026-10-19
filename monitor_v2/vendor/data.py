"""
monitor_v2/vendor/data.py

RunPod 과 Shadeform 의 **청구 API** 에서 실제로 청구된 금액을 받아온다.

end-to-end 흐름:
  1. `collect()` 가 오늘(UTC)로 시간창을 만든다 (`_windows`). 창은 셋이다 —
     오늘 하루, 이달 1일부터, 그리고 누적을 위한 월 단위 창 세 개.
  2. `_runpod()` 가 GraphQL 을 **한 번** 부른다. alias(`daily:`, `m0:`~`m2:`)로 네
     질의를 한 요청에 담는다 (`_runpod_query`). 그리고 `GET /v2/pods` 를 한 번 더
     불러 지금 도는 pod 목록을 받는다.
  3. `_shadeform()` 이 REST 를 세 번 부른다. `GET /v1/billing` 이 계정의 청구 이력
     전체(금액의 정본), `GET /v1/account` 가 잔액과 시간당 지출, 그리고
     `GET /v1/instances?status=deleted` 가 **GPU 종류**를 준다. 청구 행에는 GPU 가
     없어서 세 번째가 필요하다.
  4. 한 vendor 가 실패해도 나머지는 그대로 돌려준다. 실패한 쪽은 결과의
     `error` 에 사유가 들어가고, `vendor/report.py` 가 그 자리에 사유를 적는다.
  5. 돌려준 dict 하나를 `hyperun/report.py` 의 `send_main4_report` 가 받아서
     Main 4 의 스레드 두 장으로 그린다.

★ Main 4 본문의 숫자와 여기 숫자는 세는 대상이 다르다.
    본문은 hyperun 이 낸 PacsJob 만 세고, 금액은 카탈로그 단가 x 시간의 추정이다.
    여기는 vendor 가 실제로 청구한 금액이고 **계정 전체**다. RunPod 계정은 다른
    연구원과 함께 쓰므로(`khlee-*` pod) 차액의 일부는 우리 것이 아니다.
    2026-09-18 실측: 2026-09 hyperun 추정 $211.70 대 RunPod 청구 $482.36.

★★ 두 vendor 가 주는 가장 가는 축이 다르다. 이게 "어떤 요청에 얼마" 의 답이 vendor
    마다 다른 이유다.
    - Shadeform: instance 하나가 한 행이다. `description` 이
      `[pacsrun-<namespace>-<pod>-k3s] in <sub-provider>` 라서 job 까지 붙는다.
    - RunPod: `UserGpuCloudBilling` 에 `time` / `amount` / `gpuTypeId` 세 field
      뿐이다. `podId`, `podName`, `machineId`, `name`, `id` 를 넣으면 전부
      `Cannot query field "..." on type "UserGpuCloudBilling"` 로 거절된다
      (2026-09-18 확인). `rest.runpod.io/v1` 에는 `/billing` 경로 자체가 없다(400).
      그래서 청구를 pod 별로 못 나누고 GPU 종류까지가 한계다.
      ★ pod 목록도 **지금 살아 있는 것만** 나온다. `GET /v2/pods` 와
      `rest.runpod.io/v1/pods` 둘 다 RUNNING 만 주고, `?includeTerminated=true`,
      `?status=EXITED`, `?desiredStatus=TERMINATED` 는 모두 무시된다(같은 4행이
      온다, 2026-09-18 확인). 그래서 RunPod 쪽 목록은 "실행 내역" 이 아니라
      "지금 도는 pod" 이다.

★★★ RunPod 은 `urllib` 의 기본 User-Agent 를 403 으로 막는다.
    앞에 Cloudflare 가 있고, `Python-urllib/3.x` 로 가면 `HTTP Error 403`,
    아무 다른 문자열이면 200 이다(2026-09-18 실측, 같은 key 로 세 번 비교).
    그래서 `_USER_AGENT` 를 반드시 붙인다. Shadeform 은 기본 UA 로도 200 이다.

환경변수:
    RUNPOD_API_KEY     `rpa_...`. 없으면 RunPod 을 건너뛴다.
    SHADEFORM_API_KEY  Shadeform API key. 없으면 Shadeform 을 건너뛴다.
"""

import json
import logging
import os
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone

log = logging.getLogger(__name__)

RUNPOD_API_KEY = os.environ.get('RUNPOD_API_KEY', '').strip()
SHADEFORM_API_KEY = os.environ.get('SHADEFORM_API_KEY', '').strip()

RUNPOD_GRAPHQL = 'https://api.runpod.io/graphql'
# pod 목록만 이 host 를 쓴다. ★ 여기는 **살아 있는 pod 만** 준다 (머리말의 ★★ 참조).
RUNPOD_REST_V2 = 'https://api.runpod.io/v2'
SHADEFORM_API = 'https://api.shadeform.ai/v1'

# ★ 위의 ★★★ 참조. 이 문자열의 내용은 아무래도 좋고, "urllib 의 기본값이
# 아니어야 한다" 는 것만이 조건이다.
_USER_AGENT = 'cloud-usage-monitor/1.0'

# 20초. 두 vendor 를 합쳐 세 번의 요청이고 한 요청이 보통 1초 안쪽이다. Main 4 본문은
# 이 함수보다 먼저 만들어지므로 여기서 오래 걸려도 본문이 늦어질 뿐 사라지지는 않지만,
# Lambda 가 timeout 으로 죽으면 Main 4 자체가 못 나간다.
TIMEOUT_SECONDS = 20

# 누적을 몇 달까지 거슬러 볼지. RunPod 의 `billing` 은 한 창이 12개월을 넘으면
# `Date range cannot exceed 12 months` 로 거절하므로, 11개월짜리 창을 세 번 나눠 부른다.
# 이 계정은 2026-08 에 시작했고 그 앞 창이 0행인 것을 확인했으므로 32개월이면 넉넉하다.
# ★ 그보다 오래된 지출은 "누적" 에서 빠진다. 창을 늘리려면 alias 를 더 붙이면 된다.
_MONTH_WINDOWS = ((10, 0), (21, 10), (32, 21))


class BillingUnavailable(RuntimeError):
    """두 vendor 모두 물을 수 없었다. 호출부가 이 스레드만 건너뛰게 한다."""


def _shift_months(day: date, months: int) -> date:
    """`day` 가 속한 달의 1일에서 `months` 달 뒤로 물러난 날. 항상 그 달의 1일이다.

    창을 월 경계에 맞추려고 쓴다. RunPod 은 `startDate` 를 포함하고 `endDate` 를
    제외하므로(2026-09-18 실측: 09-04~09-05 창에 09-04 만 나온다), 창의 끝과 다음
    창의 시작을 같은 날짜로 두면 겹치지도 빠지지도 않는다.
    """
    total = (day.year * 12 + day.month - 1) - months
    return date(total // 12, total % 12 + 1, 1)


def _iso(day: date) -> str:
    return f'{day.isoformat()}T00:00:00Z'


def _runpod_query(today: date) -> str:
    """alias 다섯 개를 한 요청에 담은 GraphQL 문자열.

    - `daily`  : 이달 1일부터 내일까지, 하루 x GPU 종류. 여기서 오늘치와 MTD 를 뽑는다.
    - `m0`~`m2`: 11개월짜리 월 단위 창 셋. 세 합이 누적이다.
    """
    month_start = today.replace(day=1)
    tomorrow = today + timedelta(days=1)
    parts = [
        'clientBalance',
        'currentSpendPerHr',
        (f'daily: billing(input:{{granularity: DAILY, startDate:"{_iso(month_start)}", '
         f'endDate:"{_iso(tomorrow)}"}}) {{ gpuCloud {{ time amount gpuTypeId }} }}'),
    ]
    for index, (start_back, end_back) in enumerate(_MONTH_WINDOWS):
        start = _shift_months(today, start_back)
        end = tomorrow if end_back == 0 else _shift_months(today, end_back)
        parts.append(
            f'm{index}: billing(input:{{granularity: MONTHLY, startDate:"{_iso(start)}", '
            f'endDate:"{_iso(end)}"}}) {{ gpuCloud {{ time amount }} }}')
    return '{ myself { ' + ' '.join(parts) + ' } }'


def _post_json(url: str, body: dict, headers: dict) -> dict:
    request = urllib.request.Request(
        url, data=json.dumps(body).encode('utf-8'),
        headers={'Content-Type': 'application/json', 'User-Agent': _USER_AGENT, **headers})
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        return json.loads(response.read())


def _get_json(url: str, headers: dict) -> dict:
    request = urllib.request.Request(
        url, headers={'User-Agent': _USER_AGENT, **headers})
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        return json.loads(response.read())


def fold_runpod(me: dict, today: date) -> dict:
    """GraphQL 응답을 숫자로 접는다. HTTP 와 떼어 놓아서 test 가 진짜 이 함수를 부른다.

    Args:
        me: 응답의 `data.myself`. alias 이름(`daily`, `m0`..)이 그대로 key 다.
        today: 오늘(UTC). "오늘치" 를 가르는 기준이다.

    Returns:
        `_runpod()` 의 결과에 그대로 덮어쓸 수 있는 dict.

    누적(`total_usd`)은 `m0`~`m2` 세 창의 합이다. 세 창은 월 경계에서 맞닿고 RunPod 은
    `startDate` 를 포함하고 `endDate` 를 제외하므로(2026-09-18 실측) 겹치지도 빠지지도
    않는다. `daily` 는 이달 1일부터라 그 합이 곧 MTD 다.
    """
    folded = {
        'balance_usd': me.get('clientBalance'),
        'burn_usd_per_hour': me.get('currentSpendPerHr'),
        'today_usd': 0.0, 'mtd_usd': 0.0, 'total_usd': 0.0,
    }
    today_text = today.isoformat()
    by_gpu = {}
    for row in ((me.get('daily') or {}).get('gpuCloud') or []):
        amount = float(row.get('amount') or 0.0)
        gpu = str(row.get('gpuTypeId') or '?')
        folded['mtd_usd'] += amount
        if str(row.get('time') or '')[:10] == today_text:
            folded['today_usd'] += amount
        by_gpu[gpu] = by_gpu.get(gpu, 0.0) + amount
    folded['by_gpu'] = [{'name': name, 'usd': usd}
                        for name, usd in sorted(by_gpu.items(), key=lambda kv: -kv[1])]

    for index in range(len(_MONTH_WINDOWS)):
        for row in ((me.get(f'm{index}') or {}).get('gpuCloud') or []):
            folded['total_usd'] += float(row.get('amount') or 0.0)

    return folded


def fold_runpod_pods(pods: list) -> list:
    """`GET /v2/pods` 응답을 표 한 줄씩으로.

    ★ 여기 오는 것은 **지금 살아 있는 pod 뿐이다.** 끝난 pod 은 어떤 파라미터로도
    안 나온다(`?includeTerminated=true`, `?status=EXITED`, `?desiredStatus=TERMINATED`
    셋 다 무시되고 같은 응답이 온다, 2026-09-18 확인). 그래서 이 목록은 "실행 내역"
    이 아니라 "지금 무엇이 돈을 쓰고 있나" 다. 잔액 표의 시간당 값이 어디서 오는지가
    이 목록이다.

    `cost` 는 그 pod 의 **시간당** 청구이고 누계가 아니다.
    """
    rows = []
    for pod in pods:
        gpu = pod.get('gpu') or {}
        rows.append({
            'name': str(pod.get('name') or '?'),
            'status': str(pod.get('status') or '?'),
            'gpu': str(gpu.get('id') or gpu.get('displayName') or '?'),
            'count': int(gpu.get('count') or 0),
            'usd_per_hour': float(pod.get('cost') or 0.0),
            'started': str(pod.get('createdAt') or '')[:16].replace('T', ' '),
        })
    rows.sort(key=lambda row: -row['usd_per_hour'])
    return rows


def _runpod(today: date) -> dict:
    """RunPod 청구. GraphQL 한 번으로 오늘 / MTD / 누적 / 잔액을 모두 받는다.

    Returns:
        {'name','today_usd','mtd_usd','total_usd','balance_usd','burn_usd_per_hour',
         'by_gpu': [{'name','usd'}], 'pods': [{'name','gpu','count','usd_per_hour',
         'started'}], 'error': None}
        실패하면 금액은 0 이고 `error` 에 사유가 들어간다.
    """
    result = {'name': 'runpod', 'today_usd': 0.0, 'mtd_usd': 0.0, 'total_usd': 0.0,
              'balance_usd': None, 'burn_usd_per_hour': None,
              'by_gpu': [], 'pods': [], 'error': None}
    if not RUNPOD_API_KEY:
        result['error'] = 'RUNPOD_API_KEY 가 없다'
        return result

    try:
        answer = _post_json(f'{RUNPOD_GRAPHQL}?api_key={RUNPOD_API_KEY}',
                            {'query': _runpod_query(today)}, {})
    except urllib.error.HTTPError as exc:
        # 403 이면 거의 언제나 User-Agent 다 (이 파일 머리말의 ★★★). key 가 틀렸으면
        # 401 이나 본문의 errors 로 온다.
        result['error'] = f'HTTP {exc.code}'
        return result
    except (urllib.error.URLError, ValueError, TimeoutError) as exc:
        result['error'] = f'닿지 못했다: {exc}'
        return result

    if answer.get('errors'):
        result['error'] = str(answer['errors'][0].get('message'))[:200]
        return result

    result.update(fold_runpod((answer.get('data') or {}).get('myself') or {}, today))

    # 지금 도는 pod. 청구와 다른 경로(REST)이고, 실패해도 위의 금액은 이미 다 모였다.
    try:
        listing = _get_json(f'{RUNPOD_REST_V2}/pods',
                            {'Authorization': f'Bearer {RUNPOD_API_KEY}'})
        result['pods'] = fold_runpod_pods(listing.get('pods') or [])
    except Exception as exc:                      # noqa: BLE001
        log.warning('RunPod pod 목록을 못 읽었다: %s', exc)
    return result


def fold_shadeform(entries: list, today: date) -> dict:
    """`/v1/billing` 의 행을 금액으로 접는다. HTTP 와 떼어 놓아 test 가 직접 부른다.

    Args:
        entries: 응답의 `entries`. 계정 이력 전체가 온다.
        today: 오늘(UTC).

    Returns:
        `today_usd`, `mtd_usd`, `total_usd`.

    ★ `amount` 는 문자열이고 **청구가 음수**다. 양수는 충전(`type` 이 `payment`)이라
    지출에서 빼야 하고, 정확히 0 인 행은 몇 초만 산 instance 라 청구가 없다.

    ★★ 여기가 금액의 정본이다. instance 한 줄 한 줄은 `fold_shadeform_instances` 가
    `/v1/instances` 에서 만드는데, 두 값이 같다는 것은 확인했다 — 41행이 `id` 로 1:1
    이고 합계가 둘 다 $23.1237 이었다(2026-09-18). 그래도 합계는 청구 쪽을 쓴다.
    """
    folded = {'today_usd': 0.0, 'mtd_usd': 0.0, 'total_usd': 0.0}
    today_text = today.isoformat()
    month_text = today_text[:7]
    for entry in entries:
        try:
            amount = float(entry.get('amount') or 0.0)
        except (TypeError, ValueError):
            continue
        if amount >= 0:            # 충전이거나, 청구가 0 인 아주 짧은 instance
            continue
        spent = -amount
        stamp = str(entry.get('timestamp') or '')
        folded['total_usd'] += spent
        if stamp[:10] == today_text:
            folded['today_usd'] += spent
        if stamp[:7] == month_text:
            folded['mtd_usd'] += spent
    return folded


def _to_float(value):
    """숫자로, 못 읽으면 None. 0.0 과 "모른다" 를 가르려고 None 을 쓴다."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _shadeform_gpu(item: dict) -> str:
    """이 instance 의 GPU 이름.

    `shade_instance_type` 을 그대로 쓴다 (예: `L40S`, `RTXPro6000x4`). 여러 장짜리는
    이름이 이미 개수를 품고 있어서(`RTXPro6000x4` 의 `num_gpus` 가 4다) 개수를
    덧붙이면 겹친다.
    """
    return str(item.get('shade_instance_type')
               or (item.get('configuration') or {}).get('gpu_type') or '?')


def fold_shadeform_instances(deleted: list, live: list, today: date) -> dict:
    """`/v1/instances` 의 행을 끝난 것의 표와, 지금 도는 것의 표로.

    Args:
        deleted: `?status=deleted` 로 받은 목록.
        live: 파라미터 없이 받은 목록. 지금 살아 있는 것이다.
        today: 오늘(UTC). 끝난 것은 이달만 싣는다.

    Returns:
        `instances`(이달 끝난 것, 많이 쓴 순), `by_gpu`(같은 것의 GPU 종류별),
        `live`(지금 도는 것, 시간당 비싼 순).

    ★★★★ 지금 도는 것을 왜 갈랐나. 처음에는 한 목록에 섞었는데 그러면 두 가지가
    겹쳐서 안 보인다. (1) 방금 뜬 instance 는 `total_spend` 가 아직 0 이라 아래의
    `$0.00` 거르기에 걸려 통째로 사라진다. (2) 살아남아도 금액 순 정렬이라 26줄짜리
    표의 맨 아래, 지난 probe 들보다 밑에 놓인다. 지금 돈을 쓰고 있는 기계가 가장
    먼저 보여야 하는데 정확히 반대가 됐다.

    ★★★★★ 그래서 도는 것의 표는 금액이 아니라 **시간당 가격**(`rental_terms.
    hourly_price`)과 **시작 시각**을 축으로 쓴다. 그 둘은 뜨는 순간 정해지는 값이라
    청구가 아직 0 이어도 정확하다. `cost_estimate` 는 지금까지 얼마인지를 주는데,
    끝난 instance 에서는 `total_spend` 와 같은 값이었다(41행 전부). 도는 instance
    에서는 확인하지 못했다 — 이 코드를 쓴 2026-09-18 에 살아 있는 것이 0개였다.
    그래서 없으면 `None` 으로 두고 표에서 "-" 로 보인다.

    ★ 청구 API 에는 GPU 가 없어서 이 경로가 필요하다. `/v1/billing` 의 한 행은
    `id`, `type`, `description`, `running_time`, `status`, `amount`, `timestamp`
    뿐이고 GPU 를 가리키는 것이 없다(2026-09-18 확인). 반면 여기에는
    `shade_instance_type`(예: `L40S`, `RTXPro6000x4`)과 `configuration.gpu_type`,
    `num_gpus` 가 있다.

    이달을 가르는 기준은 `deleted_at` 이다 — 청구 행의 `timestamp` 와 같은 시각이고
    (0.02초 차이), 그래야 이 표의 합이 위의 MTD 와 같은 창을 본다.

    ★★★ 청구가 `$0.00` 으로 반올림되는 instance 는 뺀다. 2026-09 에 41개 중 15개가
    그랬는데, 전부 몇 초 만에 사라진 probe 다(`pacsrun-probe-*`, 8초/17초/19초).
    표에 `$0.00` 이 열다섯 줄 있으면 읽는 사람이 진짜 돈이 나간 줄을 못 찾고, 그
    열다섯 줄 때문에 Slack 이 표를 두 쪽으로 쪼갠다(`utils/blocks.py` 의
    `_MD_BLOCK_MAX` 가 2800자). 빠지는 금액은 다 합쳐도 $0.075 미만이라 두 자리
    소수로는 보이지도 않는다.
    """
    folded = {'instances': [], 'by_gpu': [], 'live': []}
    month_text = today.isoformat()[:7]
    by_gpu = {}
    for item in deleted:
        if str(item.get('deleted_at') or '')[:7] != month_text:
            continue
        try:
            spent = abs(float(item.get('total_spend') or 0.0))
        except (TypeError, ValueError):
            spent = 0.0
        if spent < 0.005:          # 두 자리 소수로 `$0.00` 이 되는 것
            continue
        gpu = _shadeform_gpu(item)
        seconds = int(item.get('running_time_seconds') or 0)
        folded['instances'].append({
            'name': str(item.get('name') or '?'),
            'gpu': gpu,
            'provider': str(item.get('cloud') or '?'),
            'seconds': seconds,
            'usd': spent,
            'ended': str(item.get('deleted_at') or '')[:16].replace('T', ' '),
        })
        bucket = by_gpu.setdefault(gpu, {'name': gpu, 'count': 0, 'seconds': 0, 'usd': 0.0})
        bucket['count'] += 1
        bucket['seconds'] += seconds
        bucket['usd'] += spent
    folded['instances'].sort(key=lambda row: -row['usd'])

    # 도는 것은 거르지 않는다. 방금 떠서 아직 $0 인 기계가 이 표의 주인공이다.
    for item in live:
        folded['live'].append({
            'name': str(item.get('name') or '?'),
            'gpu': _shadeform_gpu(item),
            'provider': str(item.get('cloud') or '?'),
            'status': str(item.get('status') or '?'),
            'usd_per_hour': _to_float((item.get('rental_terms') or {}).get('hourly_price')),
            'usd_so_far': _to_float(item.get('cost_estimate')),
            'started': str(item.get('created_at') or '')[:16].replace('T', ' '),
        })
    folded['live'].sort(key=lambda row: -(row['usd_per_hour'] or 0.0))
    folded['by_gpu'] = sorted(by_gpu.values(), key=lambda row: -row['usd'])
    return folded


def _shadeform(today: date) -> dict:
    """Shadeform 청구. `/v1/billing` 이 계정 이력 전체, `/v1/account` 가 잔액이다.

    `/v1/billing` 은 `limit`, `offset`, `start_date` 를 **무시한다** — 셋 다 붙여
    봐도 같은 42행이 왔다(2026-09-18 확인). 즉 창을 서버에서 못 자르고, 여기서
    `timestamp` 로 나눈다. 행이 계속 쌓이므로 언젠가 응답이 커진다.

    `amount` 는 문자열이고 **청구가 음수**다. 양수는 충전(`type` 이 `payment`)이라
    지출 합계에서 빼야 한다. 검산: 충전 $50.00 - 청구 $23.1237 = $26.8763 이고
    `/v1/account` 의 `balance` 가 `26.8763483279` 다.
    """
    result = {'name': 'shadeform', 'today_usd': 0.0, 'mtd_usd': 0.0, 'total_usd': 0.0,
              'balance_usd': None, 'burn_usd_per_hour': None,
              'instances': [], 'by_gpu': [], 'live': [], 'error': None}
    if not SHADEFORM_API_KEY:
        result['error'] = 'SHADEFORM_API_KEY 가 없다'
        return result

    headers = {'X-API-KEY': SHADEFORM_API_KEY}
    try:
        billing = _get_json(f'{SHADEFORM_API}/billing', headers)
    except urllib.error.HTTPError as exc:
        result['error'] = f'HTTP {exc.code}'
        return result
    except (urllib.error.URLError, ValueError, TimeoutError) as exc:
        result['error'] = f'닿지 못했다: {exc}'
        return result

    result.update(fold_shadeform(billing.get('entries') or [], today))

    # GPU 종류는 청구 행에 없어서 instance 목록에서 가져온다. 지워진 것과 살아 있는
    # 것을 따로 불러야 한다 — `?status=deleted` 는 지워진 것만, 파라미터가 없으면
    # 살아 있는 것만 준다. 한 번에 둘을 받는 방법은 없다(2026-09-18 확인).
    # 실패해도 위의 금액은 이미 다 모였다.
    try:
        gone = _get_json(f'{SHADEFORM_API}/instances?status=deleted', headers)
        live = _get_json(f'{SHADEFORM_API}/instances', headers)
        result.update(fold_shadeform_instances(gone.get('instances') or [],
                                               live.get('instances') or [], today))
    except Exception as exc:                      # noqa: BLE001
        log.warning('Shadeform instance 목록을 못 읽었다: %s', exc)

    # 잔액은 있으면 좋은 값이지 필수가 아니다. 여기서 실패해도 청구 숫자는 이미 다 모였다.
    try:
        account = _get_json(f'{SHADEFORM_API}/account', headers)
        result['balance_usd'] = float(account.get('balance') or 0.0)
        result['burn_usd_per_hour'] = float(account.get('hourly_burn') or 0.0)
    except Exception as exc:                      # noqa: BLE001
        log.warning('Shadeform 잔액을 못 읽었다: %s', exc)

    return result


def collect(today: date = None) -> dict:
    """두 vendor 의 청구를 모아 하나의 dict 로.

    Args:
        today: 기준일. 생략하면 오늘(UTC). **UTC 로 끊는다** — 두 vendor 의 청구가
            둘 다 UTC 이고, KST 로 끊으면 하루의 앞 9시간이 전날 것과 섞인다.

    Returns:
        {'today', 'month', 'vendors', 'total_today_usd', 'total_mtd_usd',
         'total_all_usd', 'months_covered'}
        `vendors` 는 `_runpod()` / `_shadeform()` 이 돌려준 dict 둘이다.

    Raises:
        BillingUnavailable: 두 vendor 의 key 가 **둘 다** 없다. 한 쪽만 없으면
            그 쪽에 사유를 적고 나머지는 그대로 돌려준다 — RunPod 만 쓰다가
            Shadeform 을 나중에 켜는 경우가 실제로 있었다.
    """
    if not RUNPOD_API_KEY and not SHADEFORM_API_KEY:
        raise BillingUnavailable('RUNPOD_API_KEY 와 SHADEFORM_API_KEY 가 둘 다 없다')

    day = today or datetime.now(timezone.utc).date()
    vendors = [_runpod(day), _shadeform(day)]
    return {
        'today': day.isoformat(),
        'month': day.isoformat()[:7],
        'vendors': vendors,
        'total_today_usd': sum(v['today_usd'] for v in vendors),
        'total_mtd_usd': sum(v['mtd_usd'] for v in vendors),
        'total_all_usd': sum(v['total_usd'] for v in vendors),
        'months_covered': _MONTH_WINDOWS[-1][0] + 1,
    }
