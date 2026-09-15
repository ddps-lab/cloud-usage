"""
monitor_v2/hyperun/report_test.py

Main 4 가 무엇을 그리고 무엇을 말하지 않는지.

이 파일이 지키는 것 셋:
  1. 본문에 표가 없다. Main 1 과 같은 모양이어야 하고, 표는 스레드 몫이다.
  2. 어제는 어제다. 오늘은 반만 끝났으므로 오늘 숫자를 어제라고 부르면 읽는 동안
     커지는 숫자가 된다.
  3. 금액은 어디서 읽어도 추정이라고 말한다. baseline-c 가 계산 $11.07, 청구
     $44.28 이었다. 이걸 청구서로 알고 대조하는 것이 이 보고서가 낼 수 있는 가장
     나쁜 결과다.

실행: python3 -m pytest monitor_v2/hyperun/report_test.py
"""
import json
import os
from datetime import date

import pytest

# slack.client 는 import 시점에 SLACK_BOT_TOKEN 을 읽는다. 이 파일은 블록을 만드는
# 함수만 부르고 아무것도 보내지 않으므로, 진짜 토큰이 필요하지도 않고 있어서도 안
# 된다. 자리표시자를 채워 import 만 통과시킨다.
os.environ.setdefault('SLACK_BOT_TOKEN', 'not-a-real-token')
os.environ.setdefault('SLACK_CHANNEL_ID', 'C000000000')

from monitor_v2.hyperun import report as r   # noqa: E402


def blocks_text(blocks) -> str:
    """블록 전체를 문자열 하나로. 형태가 아니라 내용에 대해 단언하려고."""
    return json.dumps([b.to_dict() if hasattr(b, 'to_dict') else b for b in blocks],
                      ensure_ascii=False)


def usage(days=None, users=None, vendors=None, mtd=1204.88, mtd_jobs=12,
          mtd_hours=190.5, note="estimate"):
    return {
        'days': days if days is not None else [
            {'date': '2026-09-14', 'jobs': 1, 'gpu_hours': 4.0,
             'estimate_usd': 25.44, 'unpriced_jobs': 0},
            {'date': '2026-09-15', 'jobs': 3, 'gpu_hours': 29.2,
             'estimate_usd': 186.11, 'unpriced_jobs': 0},
            {'date': '2026-09-16', 'jobs': 0, 'gpu_hours': 2.0,
             'estimate_usd': 12.72, 'unpriced_jobs': 0},
        ],
        'users': users if users is not None else [
            {'name': 'jglee', 'jobs': 9, 'gpu_hours': 120.0, 'estimate_usd': 763.2,
             'day_jobs': 2, 'day_gpu_hours': 25.8, 'day_estimate_usd': 164.02},
            {'name': 'max322318', 'jobs': 3, 'gpu_hours': 20.0, 'estimate_usd': 127.2,
             'day_jobs': 1, 'day_gpu_hours': 3.4, 'day_estimate_usd': 22.09},
        ],
        'teams': [{'name': 'ddps', 'jobs': 12, 'gpu_hours': 140.0, 'estimate_usd': 890.4,
                   'day_jobs': 3, 'day_gpu_hours': 29.2, 'day_estimate_usd': 186.11}],
        'vendors': vendors if vendors is not None else [
            {'name': 'runpod', 'jobs': 10, 'gpu_hours': 130.0, 'estimate_usd': 826.8,
             'day_jobs': 2, 'day_gpu_hours': 25.8, 'day_estimate_usd': 164.02},
            {'name': 'aws', 'jobs': 2, 'gpu_hours': 10.0, 'estimate_usd': 63.6,
             'day_jobs': 1, 'day_gpu_hours': 3.4, 'day_estimate_usd': 22.09},
        ],
        'mtd_estimate_usd': mtd, 'mtd_jobs': mtd_jobs, 'mtd_gpu_hours': mtd_hours,
        'note': note,
    }


TODAY = date(2026, 9, 16)


# --------------------------------------------------------------- 본문의 모양


def test_본문에는_표가_없다():
    # Main 1 과 같은 모양이어야 한다. 표는 스레드에서 찾아보는 자리이고, 본문은
    # 눈으로 훑는 자리다.
    text = blocks_text(r._build_main4(usage(), TODAY))
    assert 'markdown' not in text, '본문에 표 블록이 들어갔다'
    assert '|---' not in text


def test_절의_순서가_Main_1_과_같다():
    dumped = [b.to_dict() if hasattr(b, 'to_dict') else b
              for b in r._build_main4(usage(), TODAY)]
    kinds = [b['type'] for b in dumped]
    assert kinds[0] == 'header'
    assert kinds[-1] == 'context'
    assert kinds.count('divider') >= 3


def test_제목에_기준일과_계정이_들어간다():
    text = blocks_text(r._build_main4(usage(), TODAY))
    assert 'hyperun Job Report' in text
    assert '2026-09-15' in text, '제목의 날짜가 어제가 아니다'


# --------------------------------------------------------------- 어제는 어제다


def test_어제는_마지막으로_끝난_날이고_오늘이_아니다():
    # ★ 오늘은 반만 끝났다. 오늘 숫자를 어제라고 부르면 읽는 동안 커지는 숫자가 된다.
    text = blocks_text(r._build_main4(usage(), TODAY))
    assert '$186.11' in text, '어제(09-15)의 금액이 본문에 없다'
    assert '$12.72' not in text, '오늘(09-16)의 금액이 어제 자리에 들어갔다'


def test_그제가_있으면_어제대비를_말한다():
    text = blocks_text(r._build_main4(usage(), TODAY))
    assert '▲' in text or '▼' in text


def test_그제가_없으면_어제대비를_지어내지_않는다():
    # 창이 하루뿐이면 비교 대상이 없다. "0%" 나 "▲ +100%" 는 없는 사실을 만든 것이다.
    one_day = usage(days=[{'date': '2026-09-15', 'jobs': 1, 'gpu_hours': 4.0,
                           'estimate_usd': 25.44, 'unpriced_jobs': 0}])
    text = blocks_text(r._build_main4(one_day, TODAY))
    assert '▲' not in text and '▼' not in text


# --------------------------------------------------------------- MTD


def test_이달_누계가_본문에_있다():
    text = blocks_text(r._build_main4(usage(), TODAY))
    assert '$1,204.88' in text
    assert '이번달 누계' in text
    assert '12건' in text and '190.5시간' in text


# --------------------------------------------------------------- 사용자와 vendor


def test_사용자는_어제_쓴_순서로_다섯까지():
    text = blocks_text(r._build_main4(usage(), TODAY))
    assert text.index('jglee') < text.index('max322318')
    assert '$164.02' in text


def test_어제_아무도_안_돌렸으면_그렇게_말한다():
    quiet = usage(users=[{'name': 'jglee', 'jobs': 9, 'gpu_hours': 120.0,
                          'estimate_usd': 763.2, 'day_jobs': 0,
                          'day_gpu_hours': 0.0, 'day_estimate_usd': 0.0}])
    text = blocks_text(r._build_main4(quiet, TODAY))
    assert '어제 실행된 job 이 없습니다' in text


def test_vendor_는_한_줄로_요약한다():
    text = blocks_text(r._build_main4(usage(), TODAY))
    assert 'runpod' in text and 'aws' in text


# --------------------------------------------------------------- 정직함


def test_금액이_추정이라는_말이_반드시_남는다():
    # ★ 이 줄이 이 보고서에서 가장 중요한 한 줄이다.
    text = blocks_text(r._build_main4(usage(), TODAY))
    assert '추정' in text
    assert '$11.07' in text and '$44.28' in text, '실측 근거가 빠졌다'


def test_hyperun_으로_낸_job_만_센다고_말한다():
    text = blocks_text(r._build_main4(usage(), TODAY))
    assert 'hyperun 으로 낸 job 만' in text


def test_가격을_모르는_job_이_있으면_합계가_적다고_말한다():
    low = usage(days=[{'date': '2026-09-15', 'jobs': 2, 'gpu_hours': 8.0,
                       'estimate_usd': 25.44, 'unpriced_jobs': 1}])
    text = blocks_text(r._build_main4(low, TODAY))
    assert '1건' in text and '적습니다' in text


# --------------------------------------------------------------- 스레드


def test_스레드에는_표가_있다():
    text = blocks_text(r._build_thread('Thread 1  |  team 별', usage()['teams'], 30))
    assert 'ddps' in text


def test_일별_표는_아무것도_안_돈_날을_빼고_보인다():
    with_empty = usage(days=[
        {'date': '2026-09-10', 'jobs': 0, 'gpu_hours': 0.0, 'estimate_usd': 0.0},
        {'date': '2026-09-15', 'jobs': 3, 'gpu_hours': 29.2, 'estimate_usd': 186.11},
    ])
    text = blocks_text(r._build_thread_days(with_empty))
    assert '2026-09-15' in text
    assert '2026-09-10' not in text, '30일 창에서 빈 줄 스무 개는 표를 못 읽게 한다'


def test_일별_표가_자정을_넘긴_job_의_셈법을_설명한다():
    text = blocks_text(r._build_thread_days(usage()))
    assert '자정을 넘기면' in text
