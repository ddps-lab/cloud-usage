"""
monitor_v2/cost/forecast_test.py

'이달 예상' 이 CE forecast 없이도 나오는가.

2026-11 부터 이 계정은 MSP 빌링(Billing Transfer)이라 GetCostForecast 가
"Insufficient amount of historical data" 로 실패한다. 그동안 Main 1 / 2 / 3 은
이번 달 일평균 x 남은 일수를 잔여 예상으로 쓴다.

이 파일이 지키는 것 셋:
  1. 추정 = (d1 까지 누계 / d1 의 일자) x (d1 다음 날 ~ 말일). 누계가 없거나 말일이면 0.
  2. CE 가 값을 주면 CE 값을 쓰고, 실패하면 추정을 쓴다.
  3. 추정을 썼다는 표시는 메시지에 붙지 않는다 (CE 값과 같은 모양).

실행: python3 -m pytest monitor_v2/cost/forecast_test.py
"""
import os
from datetime import date

import pytest

os.environ.setdefault('SLACK_BOT_TOKEN', 'not-a-real-token')
os.environ.setdefault('SLACK_CHANNEL_ID', 'C000000000')

from monitor_v2.cost import data  # noqa: E402
from monitor_v2.cost.report import _build_main1  # noqa: E402


class FailingCE:
    def get_cost_forecast(self, **kw):
        raise RuntimeError('Insufficient amount of historical data.')


class OkCE:
    def get_cost_forecast(self, **kw):
        return {'Total': {'Amount': '123.45'}}


def test_estimate_is_daily_average_times_remaining_days():
    # 11-04 까지 $81.24 → 일평균 $20.31, 남은 26일
    assert data.estimate_remaining(81.24, date(2026, 11, 4)) == pytest.approx(81.24 / 4 * 26)


def test_estimate_zero_without_mtd_or_on_last_day():
    assert data.estimate_remaining(0.0, date(2026, 11, 4)) == 0.0
    assert data.estimate_remaining(500.0, date(2026, 11, 30)) == 0.0


def test_uses_ce_when_available():
    assert data.fetch_forecast_or_estimate(OkCE(), 81.24, date(2026, 11, 4)) == (123.45, 'CE')


def test_falls_back_to_estimate_when_ce_fails():
    forecast, source = data.fetch_forecast_or_estimate(FailingCE(), 81.24, date(2026, 11, 4))
    assert source == 'ESTIMATE'
    assert forecast == pytest.approx(528.06)


def test_no_forecast_when_nothing_to_estimate():
    assert data.fetch_forecast_or_estimate(FailingCE(), 0.0, date(2026, 11, 1)) == (0.0, None)


def test_main1_shows_estimate_like_ce_value():
    blocks = _build_main1(date(2026, 11, 4), {'EC2': 9.58}, {'EC2': 11.90}, 81.24, 528.06)
    text = str([b.to_dict() if hasattr(b, 'to_dict') else b for b in blocks])
    assert '$609.30' in text and '잔여 +$528.06' in text
    assert '추정' not in text and '예측 데이터 없음' not in text
