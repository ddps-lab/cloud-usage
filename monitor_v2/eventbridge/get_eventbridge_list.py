import boto3
from botocore.exceptions import ClientError, NoCredentialsError


def get_all_regions():
    """모든 AWS 리전 목록 가져오기"""
    ec2 = boto3.client('ec2', region_name='us-east-1')
    response = ec2.describe_regions(AllRegions=False)
    return [r['RegionName'] for r in response['Regions']]


def get_eventbridge_rules(region):
    """특정 리전의 EventBridge 규칙 가져오기"""
    client = boto3.client('events', region_name=region)
    rules = []
    paginator = client.get_paginator('list_rules')
    for page in paginator.paginate():
        rules.extend(page['Rules'])
    return rules


def get_eventbridge_schedules(region):
    """특정 리전의 EventBridge Scheduler 가져오기"""
    try:
        client = boto3.client('scheduler', region_name=region)
        schedules = []
        paginator = client.get_paginator('list_schedules')
        for page in paginator.paginate():
            schedules.extend(page['Schedules'])
        return schedules
    except ClientError:
        return []


def get_current_account():
    """현재 활성화된 AWS 계정 정보"""
    sts = boto3.client('sts')
    identity = sts.get_caller_identity()
    return identity['Account'], identity['Arn']


def main():
    try:
        account_id, arn = get_current_account()
        print(f"{'=' * 60}")
        print(f"현재 AWS 계정: {account_id}")
        print(f"실행 주체:     {arn}")
        print(f"{'=' * 60}\n")
    except NoCredentialsError:
        print("AWS 자격증명이 설정되지 않았습니다. aws configure를 실행해주세요.")
        return

    print("전체 리전 스캔 중...\n")
    regions = get_all_regions()

    found_any = False

    for region in sorted(regions):
        rules = get_eventbridge_rules(region)
        schedules = get_eventbridge_schedules(region)

        if rules or schedules:
            found_any = True
            print(f"\n{'=' * 60}")
            print(f"리전: {region}")
            print(f"{'=' * 60}")

            if rules:
                print(f"\n[EventBridge Rules] {len(rules)}개")
                print(f"{'이름':<45} {'상태':<10} {'스케줄'}")
                print("-" * 80)
                for rule in rules:
                    name = rule.get('Name', 'N/A')
                    state = rule.get('State', 'N/A')
                    schedule = rule.get('ScheduleExpression', '-')
                    print(f"{name:<45} {state:<10} {schedule}")

            if schedules:
                print(f"\n[EventBridge Scheduler] {len(schedules)}개")
                print(f"{'이름':<45} {'상태':<10} {'그룹'}")
                print("-" * 80)
                for schedule in schedules:
                    name = schedule.get('Name', 'N/A')
                    state = schedule.get('State', 'N/A')
                    group = schedule.get('GroupName', 'default')
                    print(f"{name:<45} {state:<10} {group}")
        else:
            print(f"리전: {region} - 없음")

    if not found_any:
        print("\nEventBridge 규칙/스케줄이 설정된 리전이 없습니다.")
    else:
        print(f"\n{'=' * 60}")
        print("스캔 완료")


if __name__ == '__main__':
    main()