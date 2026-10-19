"""
monitor_v2/vendor/report_test.py

Main 5 (neocloud 청구) 가 무엇을 말하고 무엇을 말하지 않는지.

이 파일이 지키는 것 다섯:
  1. 본문이 표 둘이다. 앞 판은 vendor 마다 한 줄씩 문장으로 늘어놓았는데, 숫자
     여섯 개가 문장에 섞여서 못 읽겠다는 지적을 받았다(2026-09-18). 열이 맞아야
     vendor 끼리 비교가 된다.
  2. 조회에 실패한 vendor 는 $0.00 이 아니라 사유를 적는다. 0 은 "안 썼다" 이고
     실패는 "모른다" 라서, 그 둘을 같은 칸에 쓰면 합계까지 못 믿게 된다.
  3. 잔액 표에 합계 행을 두지 않는다. Shadeform 의 잔액으로 RunPod 의 청구를 낼 수
     없어서, 합쳐서 나누면 남은 시간이 실제보다 길게 나온다. 2026-09-18 값으로 재면
     합산이 223.0시간, RunPod 만 보면 198.6시간이었다.
  4. 본문에 설명 문장을 넣지 않는다. 숫자를 보러 오는 자리다.
  5. hyperun 을 부르지 않는다. 그것이 Main 4 의 스레드가 아니라 따로 있는 이유다 —
     gateway 가 답하지 않는 날에도 vendor 가 청구한 금액은 사실이다.

실행: python3 -m pytest monitor_v2/vendor/report_test.py
"""
import json
import os

# slack.client 는 import 시점에 SLACK_BOT_TOKEN 을 읽는다. 이 파일은 블록을 만드는
# 함수만 부르고 아무것도 보내지 않는다.
os.environ.setdefault('SLACK_BOT_TOKEN', 'not-a-real-token')
os.environ.setdefault('SLACK_CHANNEL_ID', 'C000000000')

from monitor_v2.vendor import report as r   # noqa: E402


def as_dicts(blocks) -> list:
    return [b.to_dict() if hasattr(b, 'to_dict') else b for b in blocks]


def text_of(blocks) -> str:
    return json.dumps(as_dicts(blocks), ensure_ascii=False)


def tables_of(blocks) -> list:
    """본문의 markdown 표만. 순서는 청구, 잔액."""
    return [b['text'] for b in as_dicts(blocks) if b.get('type') == 'markdown']


def billing(runpod=None, shadeform=None):
    runpod = runpod if runpod is not None else {
        'name': 'runpod', 'today_usd': 0.75, 'mtd_usd': 482.65, 'total_usd': 671.42,
        'balance_usd': 219.28, 'burn_usd_per_hour': 1.104, 'error': None,
        'by_gpu': [{'name': 'NVIDIA A100-SXM4-80GB', 'usd': 259.33}],
        'pods': [{'name': 'khlee-sweep-14b-1', 'status': 'RUNNING',
                  'gpu': 'NVIDIA A100-SXM4-80GB', 'count': 1,
                  'usd_per_hour': 1.59, 'started': '2026-09-18 09:18'}],
    }
    shadeform = shadeform if shadeform is not None else {
        'name': 'shadeform', 'today_usd': 0.0, 'mtd_usd': 23.12, 'total_usd': 23.12,
        'balance_usd': 26.88, 'burn_usd_per_hour': 0.0, 'error': None,
        'by_gpu': [{'name': 'L40S', 'count': 12, 'seconds': 90000, 'usd': 21.504}],
        'live': [{'name': 'pacsrun-ddps-max322318-ddpsrun-aaaa1111bbbb-k3s',
                  'gpu': 'L40S', 'provider': 'hyperstack', 'status': 'active',
                  'usd_per_hour': 0.88, 'usd_so_far': 0.03,
                  'started': '2026-09-18 09:40'}],
        'instances': [{'name': 'pacsrun-ddps-ljk8540009-ddpsrun-2f374ca7fea9-k3s',
                       'gpu': 'L40S', 'provider': 'massedcompute',
                       'seconds': 26296, 'usd': 6.386, 'ended': '2026-09-12 18:24'}],
    }
    vendors = [runpod, shadeform]
    return {
        'today': '2026-09-18', 'month': '2026-09', 'vendors': vendors,
        'total_today_usd': sum(v['today_usd'] for v in vendors),
        'total_mtd_usd': sum(v['mtd_usd'] for v in vendors),
        'total_all_usd': sum(v['total_usd'] for v in vendors),
        'months_covered': 33,
    }


BROKEN_RUNPOD = {'name': 'runpod', 'today_usd': 0.0, 'mtd_usd': 0.0, 'total_usd': 0.0,
                 'balance_usd': None, 'burn_usd_per_hour': None,
                 'error': 'HTTP 403', 'by_gpu': [], 'pods': []}


def test_제목이_Neocloud_Billing_Report_다():
    assert 'Neocloud Billing Report' in text_of(r._build_main5(billing()))


def test_본문이_표_둘이다():
    tables = tables_of(r._build_main5(billing()))
    assert len(tables) == 2, '청구 표 하나와 잔액 표 하나'
    assert '| vendor | 오늘 | 이달 (2026-09) | 누적 |' in tables[0]
    assert '| vendor | 잔액 | 시간당 | 남은 시간 |' in tables[1]


def test_오늘_MTD_누적을_vendor_별로_그리고_합계까지_적는다():
    spend = tables_of(r._build_main5(billing()))[0]
    for value in ('$0.75', '$482.65', '$671.42',     # runpod
                  '$23.12',                           # shadeform
                  '$505.77', '$694.54'):              # 합계 = 위 두 줄의 합
        assert value in spend, f'{value} 가 없다'
    assert '합계' in spend


def test_조회_실패한_vendor_는_0_이_아니라_사유를_적는다():
    spend = tables_of(r._build_main5(billing(runpod=BROKEN_RUNPOD)))[0]
    assert '조회 실패: HTTP 403' in spend
    assert '| runpod | $0.00 |' not in spend, '0 은 "안 썼다" 이고 실패는 "모른다" 다'


def test_잔액_표에_합계_행이_없다():
    # Shadeform 의 잔액으로 RunPod 의 청구를 낼 수 없다. 합계 행이 있으면 읽는 사람이
    # (219.28 + 26.88) / 1.104 = 223.0시간을 직접 계산한다. 실제는 198.6시간이다.
    balance = tables_of(r._build_main5(billing()))[1]
    assert '약 198.6시간' in balance
    assert '합계' not in balance
    assert '223.0' not in balance


def test_안_쓰고_있는_vendor_의_남은_시간_칸은_비운다():
    # 시간당 $0 이면 "무한히 버틴다" 가 참이지만 아무 말도 아니다.
    balance = tables_of(r._build_main5(billing()))[1]
    shadeform_row = [line for line in balance.splitlines() if 'shadeform' in line][0]
    assert '$26.88' in shadeform_row
    assert '시간' not in shadeform_row


def test_본문에_설명_문장을_넣지_않는다():
    # 숫자를 보러 오는 자리다. 앞 판에는 추정과 청구의 차이를 푸는 문장과 hyperun
    # 대조 절이 있었는데, 본문을 길게 만들 뿐이라 뺐다 (2026-09-18).
    text = text_of(r._build_main5(billing()))
    for phrase in ('추정', '카탈로그', '다른 연구원', 'hyperun'):
        assert phrase not in text, f'{phrase} 가 본문에 남아 있다'


def test_hyperun_을_부르지_않는다():
    # Main 4 의 스레드가 아니라 따로 있는 이유가 이것이다. gateway 가 답하지 않는
    # 날에도 청구는 사실이고, 그날 이 보고서가 사라지면 안 된다. 인자가 하나뿐인
    # 것이 그 보장이다.
    import inspect
    assert list(inspect.signature(r._build_main5).parameters) == ['billing']
    assert list(inspect.signature(r.send_main5_report).parameters) == \
        ['billing', 'report_date']


def test_shadeform_스레드는_도는_것_GPU_종류별_끝난_것_순이다():
    tables = [b['text'] for b in as_dicts(r._build_thread_shadeform(billing()))
              if b.get('type') == 'markdown']
    assert len(tables) == 3
    # 지금 돈이 나가고 있는 기계가 맨 위다. 이달 집계는 지나간 일이다.
    assert '| instance | GPU | sub-provider | 시간당 | 지금까지 |' in tables[0]
    assert 'pacsrun-ddps-max322318-ddpsrun-aaaa1111bbbb-k3s' in tables[0]
    assert '| GPU | instance | 시간 | 청구 |' in tables[1]
    assert '| L40S | 12개 |' in tables[1]
    assert '| instance | GPU | sub-provider | 시간 | 청구 |' in tables[2]


def test_도는_instance_는_끝난_것_표에_섞이지_않는다():
    # 섞으면 금액 순 정렬 때문에 26줄 표의 맨 아래로 내려간다.
    tables = [b['text'] for b in as_dicts(r._build_thread_shadeform(billing()))
              if b.get('type') == 'markdown']
    assert 'aaaa1111bbbb' not in tables[2]


def test_shadeform_은_instance_이름을_통째로_싣는다():
    text = text_of(r._build_thread_shadeform(billing()))
    assert 'pacsrun-ddps-ljk8540009-ddpsrun-2f374ca7fea9-k3s' in text
    assert 'massedcompute' in text
    assert '7h18m' in text, '26296초는 사람이 쓰는 단위가 아니다'


def test_runpod_스레드는_도는_pod_이_먼저고_시간별_표가_없다():
    tables = [b['text'] for b in as_dicts(r._build_thread_runpod(billing()))
              if b.get('type') == 'markdown']
    assert len(tables) == 2
    assert '| pod | GPU | 개수 | 시간당 | 시작 시각(UTC) |' in tables[0]
    assert 'khlee-sweep-14b-1' in tables[0] and '$1.59' in tables[0]
    assert '| GPU | 청구 |' in tables[1]
    assert '시각(UTC) | GPU | 청구' not in ' '.join(tables), '시간별 표는 뺐다'


def test_runpod_pod_표의_제목이_지금_도는_pod_이다():
    # RunPod 은 끝난 pod 을 안 준다. 제목이 "이달 실행 내역" 이면 읽는 사람이 이
    # 표를 이달 전체로 오해한다.
    text = text_of(r._build_thread_runpod(billing()))
    assert '지금 도는 pod' in text
    assert '실행 내역' not in text


def test_두_vendor_의_표가_각자의_스레드로_갈린다():
    # RunPod 표에 Shadeform instance 가 섞이면, 축이 다른 두 표를 한 화면에서
    # 비교하게 되고 그 둘은 비교할 수 있는 것이 아니다.
    shadeform = text_of(r._build_thread_shadeform(billing()))
    runpod = text_of(r._build_thread_runpod(billing()))
    assert 'massedcompute' in shadeform and 'massedcompute' not in runpod
    assert 'A100-SXM4-80GB' in runpod and 'A100-SXM4-80GB' not in shadeform


def test_스레드도_조회_실패를_사유로_적는다():
    text = text_of(r._build_thread_runpod(billing(runpod=BROKEN_RUNPOD)))
    assert '조회 실패: HTTP 403' in text
