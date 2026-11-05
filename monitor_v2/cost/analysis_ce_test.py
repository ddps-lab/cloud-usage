"""
monitor_v2/cost/analysis_ce_test.py

Main 3 의 Cost Explorer fallback 이 무엇을 지켜야 하는가.

이 파일이 지키는 것 넷:
  1. CUR 에 d1 이 없거나 CUR 조회가 실패하면 CE 로 대체하고, CUR 에 d1 이 있으면
     CE 를 부르지 않는다. 대체했다는 사실(data_source / fallback_reason)이 결과에 남는다.
  2. CE 금액은 CUR 쿼리와 같은 기준이다. RECORD_TYPE 이 Credit / Refund / Tax 인 행을
     Filter 로 빼고, 서비스를 거는 조회는 그 Filter 와 SERVICE 를 And 로 묶는다.
  3. 어제 / 그제 총액은 Top N 이 아니라 모든 서비스의 합이다. 변화 Top N 은 |diff| 순이다.
  4. CE 로 대체한 메시지는 맨 위에 대체 사실을 적고, 데이터 소스를 "Cost Explorer" 로 적는다.

AWS 는 부르지 않는다. CE 는 요청을 기록하고 정해 둔 응답을 돌려주는 가짜 client 다.

실행: python3 -m pytest monitor_v2/cost/analysis_ce_test.py
"""
import os
from datetime import date

import pytest

# slack.client 는 import 시점에 SLACK_BOT_TOKEN 을 읽는다. 이 파일은 블록을 만들기만
# 하고 보내지 않으므로 가짜 값으로 충분하다 (vendor/report_test.py 와 같다).
os.environ.setdefault('SLACK_BOT_TOKEN', 'not-a-real-token')
os.environ.setdefault('SLACK_CHANNEL_ID', 'C000000000')

from monitor_v2.cost import analysis, analysis_ce  # noqa: E402
from monitor_v2.cost import data as cost_data  # noqa: E402
from monitor_v2.cost.report_analysis import _build_main3

D1 = date(2026, 11, 1)
D2 = date(2026, 10, 31)


def _group(keys, cost, qty=0.0):
    return {'Keys': keys, 'Metrics': {
        'UnblendedCost': {'Amount': str(cost)}, 'UsageQuantity': {'Amount': str(qty)}}}


class FakeCE:
    """get_cost_and_usage 요청을 기록하고, pages 의 응답을 차례로 돌려준다."""
    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = []

    def get_cost_and_usage(self, **kwargs):
        self.calls.append(kwargs)
        return self.pages.pop(0)


# ---------------------------------------------------------------------------
# 2. 금액 기준과 CE 호출 모양
# ---------------------------------------------------------------------------

def test_ce_rows_paginates_and_strips_tag_prefix():
    ce = FakeCE([
        {'ResultsByTime': [{'TimePeriod': {'Start': '2026-11-01'}, 'Groups': [
            _group(['aws:createdBy$IAMUser:AIDA:woohyeok', 'BoxUsage'], 2.0)]}],
         'NextPageToken': 'p2'},
        {'ResultsByTime': [{'TimePeriod': {'Start': '2026-11-01'}, 'Groups': [
            _group(['aws:createdBy$', 'BoxUsage'], 1.0)]}]},
    ])
    rows = analysis_ce._ce_rows(ce, D1, D1, 'DAILY', ('aws:createdBy', 'USAGE_TYPE'),
                                service='EC2')
    assert [r['keys'] for r in rows] == [['IAMUser:AIDA:woohyeok', 'BoxUsage'], ['', 'BoxUsage']]
    assert ce.calls[1]['NextPageToken'] == 'p2'
    # End 는 exclusive 라 d1 다음 날이다.
    assert ce.calls[0]['TimePeriod'] == {'Start': '2026-11-01', 'End': '2026-11-02'}
    assert ce.calls[0]['GroupBy'] == [
        {'Type': 'TAG', 'Key': 'aws:createdBy'}, {'Type': 'DIMENSION', 'Key': 'USAGE_TYPE'}]


def test_filter_excludes_credit_refund_tax_and_ands_service():
    assert analysis_ce._filter() == {'Not': {'Dimensions': {
        'Key': 'RECORD_TYPE', 'Values': ['Credit', 'Refund', 'Tax']}}}
    f = analysis_ce._filter('EC2 - Other')
    assert analysis_ce._filter() in f['And']
    assert {'Dimensions': {'Key': 'SERVICE', 'Values': ['EC2 - Other']}} in f['And']


# ---------------------------------------------------------------------------
# 3. 총액과 변화 Top N
# ---------------------------------------------------------------------------

def test_daily_totals_sum_every_service_and_diff_sorted_by_abs():
    ce = FakeCE([{'ResultsByTime': [
        {'TimePeriod': {'Start': '2026-10-31'}, 'Groups': [
            _group(['EC2'], 10.0), _group(['S3'], 5.0), _group(['Route 53'], 1.0)]},
        {'TimePeriod': {'Start': '2026-11-01'}, 'Groups': [
            _group(['EC2'], 4.0), _group(['S3'], 7.0), _group(['Route 53'], 1.0)]},
    ]}])
    d1_total, d2_total, rows = analysis_ce.fetch_daily_totals_ce(ce, D1, D2)
    assert (d1_total, d2_total) == (12.0, 16.0)
    # Route 53 은 변화가 0 이라 빠지지만 총액에는 들어간다.
    assert [(r['service'], r['diff']) for r in rows] == [('EC2', -6.0), ('S3', 2.0)]


def test_service_mtd_includes_d1_on_first_of_month():
    ce = FakeCE([{'ResultsByTime': [{'TimePeriod': {'Start': '2026-11-01'}, 'Groups': [
        _group(['EC2'], 20.0), _group(['S3'], 0.001)]}]}])
    mtd_total, service_mtd = analysis_ce.fetch_service_mtd_ce(ce, D1)
    assert ce.calls[0]['TimePeriod'] == {'Start': '2026-11-01', 'End': '2026-11-02'}
    assert mtd_total == pytest.approx(20.001)
    assert service_mtd == {'EC2': pytest.approx(20.0 * analysis._TAX_MULTIPLIER)}


def test_new_costs_empty_on_first_of_month():
    assert analysis_ce.fetch_month_new_costs_ce(FakeCE([]), D1) == []


def test_top_users_skip_untagged():
    ce = FakeCE([{'ResultsByTime': [{'TimePeriod': {'Start': '2026-11-01'}, 'Groups': [
        _group(['aws:createdBy$IAMUser:AIDA:woohyeok', 'RDS'], 2.4),
        _group(['aws:createdBy$', 'EKS'], 9.0),
    ]}]}])
    users = analysis_ce.fetch_mtd_top_users_with_breakdown_ce(ce, D1)
    assert [u['iam_user'] for u in users] == ['woohyeok']


# ---------------------------------------------------------------------------
# 1. 언제 CE 로 대체하는가
# ---------------------------------------------------------------------------

_CE_RAW = {
    'd1_total': 34.39, 'd2_total': 20.46, 'service_rows': [], 'usage_type_rows': [],
    'resource_rows': [], 'top_services': [], 'new_costs': [], 'service_mtd': {},
    'top_users_mtd': [], 'mtd_total': 34.39,
}


@pytest.fixture
def no_aws(monkeypatch):
    """boto3 / forecast / Bedrock 을 끊고, CE 수집이 불렸는지 기록한다."""
    called = []
    monkeypatch.setattr(analysis.boto3, 'client', lambda *a, **k: object())
    monkeypatch.setattr(cost_data, 'fetch_cost_forecast', lambda ce: 0.0)
    monkeypatch.setattr(analysis, 'summarize', lambda **k: '요약')
    monkeypatch.setattr(analysis_ce, 'collect_raw_ce',
                        lambda *a: called.append(a) or dict(_CE_RAW))
    return called


def test_falls_back_to_ce_when_cur_has_no_d1(monkeypatch, no_aws):
    monkeypatch.setattr(analysis, '_collect_raw_cur', lambda *a: None)
    out = analysis.collect_all(D1)
    assert len(no_aws) == 1
    assert out['data_source'] == 'CE'
    assert '2026-11-01' in out['fallback_reason']
    assert out['d1_total'] == 34.39


def test_falls_back_to_ce_when_cur_query_fails(monkeypatch, no_aws):
    def boom(*a):
        raise TimeoutError('Athena 쿼리 타임아웃')
    monkeypatch.setattr(analysis, '_collect_raw_cur', boom)
    out = analysis.collect_all(D1)
    assert out['data_source'] == 'CE'
    assert 'TimeoutError' in out['fallback_reason']


def test_uses_cur_when_d1_present(monkeypatch, no_aws):
    monkeypatch.setattr(analysis, '_collect_raw_cur', lambda *a: {**_CE_RAW, 'd1_total': 30.0})
    out = analysis.collect_all(D1)
    assert no_aws == []
    assert out['data_source'] == 'CUR'
    assert out['fallback_reason'] is None


# ---------------------------------------------------------------------------
# 4. 메시지에 대체 사실이 보이는가
# ---------------------------------------------------------------------------

def _texts(blocks):
    return str([b.to_dict() if hasattr(b, 'to_dict') else b for b in blocks])


def test_ce_message_states_fallback_and_source(monkeypatch, no_aws):
    monkeypatch.setattr(analysis, '_collect_raw_cur', lambda *a: None)
    text = _texts(_build_main3(analysis.collect_all(D1)))
    assert ('⚠️ CUR 에 2026-11-01 데이터가 아직 없음 → Cost Explorer 값으로 대체했습니다, '
            'aws:createdBy 기준이라 상세한 IAM User 구분은 되지 않을 수 있습니다.') in text
    assert '데이터 소스: Cost Explorer (CUR 대체)' in text
    assert 'Cost Explorer 는 리소스 단위 미제공' in text


def test_ce_notice_date_follows_report_date(monkeypatch, no_aws):
    monkeypatch.setattr(analysis, '_collect_raw_cur', lambda *a: None)
    text = _texts(_build_main3(analysis.collect_all(date(2026, 12, 1))))
    assert '⚠️ CUR 에 2026-12-01 데이터가 아직 없음' in text
    assert '2026-11-01' not in text


def test_cur_message_has_no_fallback_notice(monkeypatch, no_aws):
    monkeypatch.setattr(analysis, '_collect_raw_cur', lambda *a: dict(_CE_RAW))
    text = _texts(_build_main3(analysis.collect_all(D1)))
    assert '대체' not in text
    assert '데이터 소스: CUR' in text
