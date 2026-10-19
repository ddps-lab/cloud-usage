"""
monitor_v2/vendor/data_test.py

vendor 청구를 숫자로 접는 부분이 무엇을 지켜야 하는가.

이 파일이 지키는 것 넷:
  1. 누적을 세는 세 창이 월 경계에서 맞닿아 겹치지도 빠지지도 않는다. RunPod 이
     `startDate` 를 포함하고 `endDate` 를 제외하기 때문에 성립하는 성질이라,
     경계를 하루라도 어긋나게 잡으면 한 달치가 두 번 세어지거나 통째로 빠진다.
  2. 충전은 지출이 아니다. Shadeform 의 `amount` 는 청구가 음수이고 충전이 양수라,
     부호를 안 보면 $50 충전이 지출 $50 이 된다.
  3. MTD 와 "오늘" 이 UTC 로 갈린다. 두 vendor 의 청구가 둘 다 UTC 이고, KST 로
     끊으면 하루의 앞 9시간이 전날 것과 섞인다.
  4. instance 이름을 통째로 싣는다. 잘라 쓰면 `kubectl` 결과나 driver 로그에서
     같은 이름을 다시 찾을 수 없다.

HTTP 는 부르지 않는다. `fold_runpod` 와 `fold_shadeform` 이 순수 함수라서
가짜 응답 없이 진짜 함수를 그대로 부른다.

실행: python3 -m pytest monitor_v2/vendor/data_test.py
"""
from datetime import date

from monitor_v2.vendor.data import (
    _MONTH_WINDOWS, _runpod_query, _shift_months, fold_runpod, fold_runpod_pods,
    fold_shadeform, fold_shadeform_instances,
)

TODAY = date(2026, 9, 18)


def test_누적_창_셋이_월_경계에서_맞닿는다():
    # RunPod 은 startDate 를 포함하고 endDate 를 제외한다 (2026-09-18 실측:
    # 09-04~09-05 창에 09-04 만 나온다). 그래서 앞 창의 끝과 뒤 창의 시작이 같은
    # 날짜여야 겹치지도 빠지지도 않는다.
    starts = [_shift_months(TODAY, back) for back, _ in _MONTH_WINDOWS]
    ends = [_shift_months(TODAY, forward) for _, forward in _MONTH_WINDOWS]
    assert starts == [date(2025, 11, 1), date(2024, 12, 1), date(2024, 1, 1)]
    # m0 의 시작 == m1 의 끝, m1 의 시작 == m2 의 끝
    assert starts[0] == ends[1]
    assert starts[1] == ends[2]
    # 모든 창이 그 달의 1일에서 시작한다
    assert all(day.day == 1 for day in starts)


def test_한_창이_12개월을_넘지_않는다():
    # 넘으면 RunPod 이 `Date range cannot exceed 12 months` 로 거절한다.
    for back, forward in _MONTH_WINDOWS:
        assert back - forward <= 11, '창 하나가 11개월을 넘었다'


def test_질의가_이달_1일부터_내일까지를_본다():
    query = _runpod_query(TODAY)
    assert 'startDate:"2026-09-01T00:00:00Z"' in query
    assert 'endDate:"2026-09-19T00:00:00Z"' in query, '오늘치가 들어오려면 내일까지여야 한다'
    # alias 넷이 한 요청에 담겨야 조회가 한 번으로 끝난다
    assert query.count('billing(input:') == 1 + len(_MONTH_WINDOWS)


def test_runpod_오늘과_MTD_와_누적을_따로_센다():
    me = {
        'clientBalance': 219.28, 'currentSpendPerHr': 1.104,
        'daily': {'gpuCloud': [
            {'time': '2026-09-17T00:00:00.000Z', 'amount': 10.0, 'gpuTypeId': 'L40S'},
            {'time': '2026-09-18T00:00:00.000Z', 'amount': 2.0, 'gpuTypeId': 'L40S'},
            {'time': '2026-09-18T00:00:00.000Z', 'amount': 1.0, 'gpuTypeId': 'A100'},
        ]},
        'm0': {'gpuCloud': [{'time': '2026-09-01T00:00:00.000Z', 'amount': 13.0}]},
        'm1': {'gpuCloud': [{'time': '2025-03-01T00:00:00.000Z', 'amount': 7.0}]},
        'm2': {'gpuCloud': []},
    }
    folded = fold_runpod(me, TODAY)
    assert folded['today_usd'] == 3.0, '오늘은 09-18 의 두 줄만'
    assert folded['mtd_usd'] == 13.0, 'daily 창이 이달 1일부터라 그 합이 MTD 다'
    assert folded['total_usd'] == 20.0, '누적은 세 창의 합'
    assert folded['balance_usd'] == 219.28


def test_runpod_은_GPU_종류로만_나뉜다():
    # pod 별 분해는 vendor 가 주지 않는다. 많이 쓴 순으로 정렬해서, 어느 GPU 가
    # 돈을 먹었는지가 첫 줄에 오게 한다.
    me = {'daily': {'gpuCloud': [
        {'time': '2026-09-18T00:00:00.000Z', 'amount': 1.0, 'gpuTypeId': 'L4'},
        {'time': '2026-09-17T00:00:00.000Z', 'amount': 9.0, 'gpuTypeId': 'A100'},
        {'time': '2026-09-18T00:00:00.000Z', 'amount': 5.0, 'gpuTypeId': 'A100'},
    ]}}
    folded = fold_runpod(me, TODAY)
    assert folded['by_gpu'] == [{'name': 'A100', 'usd': 14.0}, {'name': 'L4', 'usd': 1.0}]


def test_runpod_pod_목록은_지금_도는_것만이다():
    # RunPod 은 끝난 pod 을 어떤 파라미터로도 안 준다 (2026-09-18 확인). 이 함수가
    # 받는 것도 살아 있는 pod 뿐이고, `cost` 는 누계가 아니라 시간당 값이다.
    pods = fold_runpod_pods([
        {'name': 'khlee-sweep-8b-1b', 'status': 'RUNNING', 'cost': 1.09,
         'gpu': {'id': 'NVIDIA L40S', 'count': 1},
         'createdAt': '2026-09-18T08:47:35.857Z'},
        {'name': 'khlee-sweep-14b-1', 'status': 'RUNNING', 'cost': 1.59,
         'gpu': {'id': 'NVIDIA A100-SXM4-80GB', 'count': 1},
         'createdAt': '2026-09-18T09:18:58.281Z'},
    ])
    # 비싼 것이 위로. 돈이 어디서 나가는지가 첫 줄에 와야 한다.
    assert [p['name'] for p in pods] == ['khlee-sweep-14b-1', 'khlee-sweep-8b-1b']
    assert pods[0]['gpu'] == 'NVIDIA A100-SXM4-80GB' and pods[0]['count'] == 1
    assert pods[0]['usd_per_hour'] == 1.59
    assert pods[0]['started'] == '2026-09-18 09:18'


def test_shadeform_충전을_지출로_세지_않는다():
    # `amount` 는 문자열이고 청구가 음수다. 부호를 안 보면 $50 충전이 지출이 된다.
    entries = [
        {'type': 'payment', 'description': 'Wallet top-up  of $50', 'running_time': -1,
         'status': 'completed', 'amount': '50', 'timestamp': '2026-09-09T04:32:07Z'},
        {'type': 'instance', 'description': '[pacsrun-a-k3s] in hyperstack',
         'running_time': 3600, 'status': 'deleted', 'amount': '-1.25',
         'timestamp': '2026-09-18T06:05:16Z'},
    ]
    folded = fold_shadeform(entries, TODAY)
    assert folded['total_usd'] == 1.25
    assert folded['today_usd'] == 1.25
    assert folded['mtd_usd'] == 1.25, '충전 $50 이 지출로 들어가면 안 된다'


def test_shadeform_지난달은_누적에만_들어가고_MTD_에는_안_들어간다():
    entries = [
        {'type': 'instance', 'amount': '-4.00', 'running_time': 100,
         'timestamp': '2026-08-20T01:00:00Z', 'description': '[old] in crusoe'},
        {'type': 'instance', 'amount': '-1.00', 'running_time': 100,
         'timestamp': '2026-09-05T01:00:00Z', 'description': '[new] in crusoe'},
    ]
    folded = fold_shadeform(entries, TODAY)
    assert folded['total_usd'] == 5.0
    assert folded['mtd_usd'] == 1.0


def instance(name, gpu, usd, seconds=600, cloud='massedcompute',
             deleted='2026-09-12T18:24:00Z'):
    return {'name': name, 'shade_instance_type': gpu, 'cloud': cloud,
            'configuration': {'gpu_type': gpu.rstrip('x0123456789'), 'num_gpus': 1},
            'total_spend': f'-{usd}', 'running_time_seconds': seconds,
            'created_at': '2026-09-12T11:00:00Z', 'deleted_at': deleted}


def test_shadeform_GPU_종류는_instance_목록에서_온다():
    # 청구 API 에는 GPU 가 없다. `/v1/billing` 한 행에 `id`, `type`, `description`,
    # `running_time`, `status`, `amount`, `timestamp` 뿐이다 (2026-09-18 확인).
    folded = fold_shadeform_instances([
        instance('a-k3s', 'L40S', 6.386, 26296),
        instance('b-k3s', 'L40S', 3.577, 14889),
        instance('c-k3s', 'A6000', 0.220, 1810, cloud='hyperstack'),
    ], [], TODAY)
    assert [g['name'] for g in folded['by_gpu']] == ['L40S', 'A6000']
    assert folded['by_gpu'][0]['count'] == 2
    assert folded['by_gpu'][0]['seconds'] == 26296 + 14889
    assert abs(folded['by_gpu'][0]['usd'] - 9.963) < 1e-9


def test_shadeform_instance_이름을_통째로_싣는다():
    # ★ namespace 가 `ddps-ljk8540009` 처럼 `-` 를 품고 있어서 조각내면 다시 못
    # 맞추고, 무엇보다 이 문자열 그대로 kubectl 과 driver 로그에 찍힌다.
    row = fold_shadeform_instances(
        [instance('pacsrun-ddps-ljk8540009-ddpsrun-2f374ca7fea9-k3s', 'L40S', 6.386, 26296)],
        [], TODAY)['instances'][0]
    assert row['name'] == 'pacsrun-ddps-ljk8540009-ddpsrun-2f374ca7fea9-k3s'
    assert row['gpu'] == 'L40S'
    assert row['provider'] == 'massedcompute'
    assert row['usd'] == 6.386
    assert row['ended'] == '2026-09-12 18:24'


def test_shadeform_청구가_0으로_반올림되는_instance_는_뺀다():
    # 몇 초만 산 probe 다. 2026-09 에 41개 중 15개가 그랬고, 표에 `$0.00` 이 열다섯
    # 줄 있으면 진짜 돈이 나간 줄을 못 찾는다.
    folded = fold_shadeform_instances([
        instance('pacsrun-probe-c2', 'RTXPro6000x4', 0.0, 8),
        instance('pacsrun-probe-c1', 'L40S', 0.004, 17),
        instance('real-k3s', 'L40S', 6.386, 26296),
    ], [], TODAY)
    assert [row['name'] for row in folded['instances']] == ['real-k3s']
    assert [g['name'] for g in folded['by_gpu']] == ['L40S']


def test_shadeform_지난달_instance_는_표에_안_들어간다():
    folded = fold_shadeform_instances([
        instance('old-k3s', 'L40S', 4.0, 600, deleted='2026-08-20T01:00:00Z'),
        instance('new-k3s', 'L40S', 1.0, 600),
    ], [], TODAY)
    assert [row['name'] for row in folded['instances']] == ['new-k3s']


def test_도는_instance_는_청구가_0이어도_목록에_남는다():
    # ★ 이것이 이 파일에서 가장 중요한 한 줄이다. 방금 뜬 instance 는 `total_spend`
    # 가 아직 0 이라, 끝난 것과 같은 목록에 넣으면 `$0.00` 거르기에 걸려 통째로
    # 사라진다. 지금 돈을 쓰고 있는 기계가 안 보이는 것이 최악이다.
    live = dict(instance('live-k3s', 'L40S', 0.0, 120), deleted_at=None,
                status='active', cost_estimate='0',
                rental_terms={'rental_type': 'on_demand', 'hourly_price': '0.88'})
    folded = fold_shadeform_instances([], [live], TODAY)
    assert folded['instances'] == [], '끝난 것 표에는 안 들어간다'
    assert [row['name'] for row in folded['live']] == ['live-k3s']
    assert folded['live'][0]['usd_per_hour'] == 0.88, '시간당은 뜨는 순간 정해진다'
    assert folded['live'][0]['started'] == '2026-09-12 11:00'


def test_도는_instance_의_지금까지_값이_없으면_0이_아니라_모른다():
    # 0.0 을 돌려주면 표에 `$0.00` 으로 찍혀서 "안 썼다" 로 읽힌다.
    live = dict(instance('live-k3s', 'L40S', 0.0), deleted_at=None, cost_estimate=None)
    row = fold_shadeform_instances([], [live], TODAY)['live'][0]
    assert row['usd_so_far'] is None


def test_도는_instance_는_시간당_비싼_순이다():
    def live(name, price):
        return dict(instance(name, 'L40S', 0.0), deleted_at=None,
                    rental_terms={'hourly_price': price})
    rows = fold_shadeform_instances([], [live('a', '0.5'), live('b', '8.76')], TODAY)['live']
    assert [row['name'] for row in rows] == ['b', 'a']
