"""
monitor_v2/test_error_alert.py

slack.post_error 의 Block Kit 메시지 포맷을 Slack 채널에서 육안으로 확인하기 위한
로컬 테스트 스크립트.

실제 lambda_handler 흐름은 거치지 않고, 의도적으로 예외를 발생시킨 뒤
post_error 를 직접 호출한다. AWS / Athena / Bedrock 호출 없이 Slack 발송만 일어난다.

⚠️ 실행하면 .env 의 SLACK_CHANNEL_ID 채널로 진짜 메시지가 발송된다.
   운영 채널이면 테스트 채널로 임시 변경 후 실행할 것.

실행:
    uv run python -m monitor_v2.test_error_alert
    uv run python -m monitor_v2.test_error_alert --stage cost_collect
    uv run python -m monitor_v2.test_error_alert --stage all
"""

import argparse

from print_test.utils.environment import setup_environment
setup_environment()

from monitor_v2.slack import client as slack


# ---------------------------------------------------------------------------
# 호출 스택을 깊게 만들어 traceback 마지막 10줄 노출 기능이 잘 보이도록 한다.
# 각 단계별로 서로 다른 예외 타입/메시지를 발생시켜 실제 운영 상황을 모사한다.
# ---------------------------------------------------------------------------

def _athena_query(sql: str):
    raise RuntimeError(f"Athena 쿼리 실패 — table 'hyu_ddps_logs.cur_logs' partition not found ({sql[:30]}...)")


def _bedrock_invoke(model_id: str):
    raise RuntimeError(f"Bedrock {model_id} ThrottlingException: Rate exceeded")


def _slack_post(blocks):
    raise ValueError(f"invalid_blocks: text length 12345 exceeds 10000 limit (blocks={len(blocks)})")


def _ec2_describe():
    d = {'instances': []}
    return d['Instances']  # KeyError 유도


def _scenario_cost_collect():
    _athena_query("SELECT ... FROM cur_logs WHERE year=2026 AND month=5")


def _scenario_cost_report():
    _slack_post([{'type': 'header'}, {'type': 'section'}] * 30)


def _scenario_ec2_collect():
    _ec2_describe()


def _scenario_ec2_report():
    _slack_poxst([{'type': 'markdown'}] * 50)


def _scenario_ai_analysis():
    _bedrock_invoke('amazon.nova-micro-v1:0')


SCENARIOS = {
    'cost_collect':  (_scenario_cost_collect,  {'report_type': 'all',
                                                'account_id':  '320674564649'}),
    'cost_report':   (_scenario_cost_report,   {'report_type': 'all',
                                                'account_id':  '320674564649', 'date': '2026-05-17'}),
    'ec2_collect':   (_scenario_ec2_collect,   {'report_type': 'all',
                                                'account_id':  '320674564649', 'date': '2026-05-17'}),
    'ec2_report':    (_scenario_ec2_report,    {'report_type': 'ec2',
                                                'account_id':  '320674564649', 'date': '2026-05-17'}),
    'ai_analysis':   (_scenario_ai_analysis,   {'report_type': 'analysis',
                                                'account_id':  '786382940258', 'date': '2026-05-16'}),
}


def trigger(stage: str) -> None:
    """주어진 단계의 시나리오 함수를 호출해 예외를 일으키고 post_error 로 알림 전송."""
    scenario_fn, meta = SCENARIOS[stage]
    try:
        scenario_fn()
    except Exception as e:
        slack.post_error(context=stage, error=e, meta=meta)
        print(f"[test_error_alert] stage={stage} → Slack 알림 발송 완료 ({type(e).__name__})")


def main():
    parser = argparse.ArgumentParser(description='post_error Block Kit 포맷 테스트')
    parser.add_argument(
        '--stage', default='cost_collect',
        choices=list(SCENARIOS.keys()) + ['all'],
        help="발송할 시나리오. 'all' 지정 시 모든 단계를 순차 발송.",
    )
    args = parser.parse_args()

    stages = list(SCENARIOS.keys()) if args.stage == 'all' else [args.stage]
    for s in stages:
        trigger(s)

    print(f"[test_error_alert] 완료 — 발송 단계: {stages}")


if __name__ == '__main__':
    main()
