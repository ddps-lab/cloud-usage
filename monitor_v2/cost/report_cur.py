"""
monitor_v2/cost/report_cur.py

Athena CUR 기반 Cost 리포트 발송 진입점.

data_cur.py 의 collect_all() 이 반환하는 dict 구조는
data.py 의 collect_all() 과 동일하므로 report.py 의 블록 빌더를 그대로 사용한다.

사용 예:
    from monitor_v2.cost.data_cur import collect_all
    from monitor_v2.cost.report_cur import send_cur_report

    cost_data = collect_all(today_kst)
    send_cur_report(cost_data)
"""

from .report import send_main1_report as _send


def send_cur_report(cost_data: dict) -> None:
    """
    Athena CUR 데이터 기반 Cost 리포트를 Slack으로 발송한다.

    Args:
        cost_data: data_cur.collect_all() 의 반환값
    """
    _send(cost_data)
