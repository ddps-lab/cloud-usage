"""
monitor_v2/hyperun/data.py

hyperun gateway 에서 일별 사용량을 받아온다.

왜 gateway 에게 묻는가:
    이 Lambda 는 cluster 밖에 있고 PacsJob 을 직접 읽을 수 없다. gateway 는 이미
    cluster 권한과 사람 목록을 들고 있으므로, 집계는 거기서 하고 여기서는 그 답을
    받아 그린다. 같은 숫자를 두 곳에서 계산하면 언젠가 갈라진다.

무엇이 오는가:
    GET /v1/usage?days=N 이 날짜별 합계와 team / user / vendor 별 집계, 그리고
    이달 누계(MTD)를 준다. 전문은 hyperun 의 agent/references/api.md.

★ 여기 오는 돈은 전부 추정이다:
    카탈로그 단가 x 시간이다. vendor 의 청구서는 다른 숫자이고 실측으로 크게
    달랐다 — baseline-c 가 계산 $11.07, 청구 $44.28(카드 수를 빠뜨림). 그래서
    field 이름이 estimate 이고, 화면에도 "추정" 이라고 적는다.

★★ hyperun 으로 낸 job 만 센다:
    PacsJob 에서만 읽으므로 같은 RunPod 계정에서 누가 손으로 띄운 pod 은 들어오지
    않는다. 그게 맞는 동작이다: 이 보고서는 "hyperun 이 쓴 돈" 을 말한다.

환경변수:
    HYPERUN_API_BASE    gateway 주소. 없으면 이 보고서를 건너뛴다.
    HYPERUN_API_TOKEN   operator 토큰. /v1/usage 는 operator 전용이다.
    HYPERUN_USAGE_DAYS  며칠치를 받을지. 기본 30.
"""

import json
import os
import urllib.error
import urllib.request

API_BASE = os.environ.get('HYPERUN_API_BASE', '').rstrip('/')
API_TOKEN = os.environ.get('HYPERUN_API_TOKEN', '').strip()
USAGE_DAYS = int(os.environ.get('HYPERUN_USAGE_DAYS', '30'))

# 30초. gateway 는 namespace 마다 apiserver 를 한 번씩 조회하므로 한 자릿수 초가
# 보통이고, 그보다 오래 걸리면 답을 기다리는 것보다 보고서를 거르는 편이 낫다 —
# Lambda 가 timeout 으로 죽으면 Main 1 과 Main 2 까지 같이 못 나간다.
TIMEOUT_SECONDS = 30


class UsageUnavailable(RuntimeError):
    """gateway 에게 물을 수 없었다. 호출부가 이 보고서만 건너뛰게 한다."""


def collect(days: int = None) -> dict:
    """gateway 에서 사용량을 받아 그대로 돌려준다.

    Args:
        days: 며칠치. 생략하면 HYPERUN_USAGE_DAYS.

    Returns:
        UsageResponse 본문. `days`, `teams`, `users`, `vendors`,
        `mtd_estimate_usd`, `mtd_gpu_hours`, `mtd_jobs`, `note` 를 담는다.

    Raises:
        UsageUnavailable: 주소나 토큰이 없거나, gateway 가 답하지 않았다.
            설정이 안 된 것과 고장난 것을 같은 예외로 묶은 이유는 호출부가 할 일이
            둘 다 같기 때문이다 — 이 보고서를 건너뛰고 나머지를 보낸다.
    """
    if not API_BASE or not API_TOKEN:
        raise UsageUnavailable(
            'HYPERUN_API_BASE 와 HYPERUN_API_TOKEN 이 설정되지 않았다')

    how_many = USAGE_DAYS if days is None else days
    request = urllib.request.Request(
        f'{API_BASE}/v1/usage?days={how_many}',
        headers={'Authorization': f'Bearer {API_TOKEN}'})
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as exc:
        # 403 은 토큰이 operator 가 아니라는 뜻이고, 그건 설정 문제라 재시도해도
        # 같은 답이 온다. 메시지에 status 를 넣어 로그만 보고 알 수 있게 한다.
        raise UsageUnavailable(f'gateway 가 {exc.code} 로 거절했다') from exc
    except (urllib.error.URLError, ValueError, TimeoutError) as exc:
        raise UsageUnavailable(f'gateway 에 닿지 못했다: {exc}') from exc
