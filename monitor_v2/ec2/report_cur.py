"""
monitor_v2/ec2/report_cur.py

Athena CUR 기반 EC2 리포트 발송 진입점.

ec2/data_cur.py 의 collect_all() 반환 구조는
ec2/data.py 의 collect_all() 과 동일하므로 report.py 의 블록 빌더를 그대로 사용한다.

사용 예:
    from monitor_v2.ec2.data_cur import collect_all as collect_ec2_data
    from monitor_v2.ec2.report_cur import send_ec2_cur_report

    ec2_data = collect_ec2_data(regions, account_id, d1_date)
    send_ec2_cur_report(cost_data, ec2_data)
"""

from .report import send_main2_report as _send


def send_ec2_cur_report(cost_data: dict, ec2_data: dict) -> None:
    """
    Athena CUR 데이터 기반 EC2 리포트를 Slack으로 발송한다.

    Args:
        cost_data: cost/data_cur.collect_all() 의 반환값
        ec2_data:  ec2/data_cur.collect_all()  의 반환값
    """
    _send(cost_data, ec2_data)
