import boto3
from datetime import datetime, timezone, timedelta
import urllib.request, urllib.parse, json
import os, re, sys


SLACK_URL = os.environ['SLACK_URL']

# enable instance management - 모든 리전의 인스턴스와 볼륨을 조회한 후 리스트로 반환한다.
def enable_instance_management(current_time, head_message):
    try:
        # regions 검색용 Boto3 EC2 클라이언트 생성
        ec2_client = boto3.client('ec2')
        regions = [ region['RegionName'] for region in ec2_client.describe_regions()['Regions'] ]

        running_instances, stopped_instances = get_instance_items(current_time, head_message, regions)
        volume_list = get_volume_items(current_time, head_message, regions)

        return running_instances, stopped_instances, volume_list
    except Exception as e:
        send_exception_message(head_message, "오류가 발생했습니다.\n", e)


# get instance items - 모든 리전의 인스턴스를 조회한 후 리스트로 반환한다.
def get_instance_items(current_time, head_message, regions):
    try:
        running_instances = []
        stopped_instances = []

        # 리전 별로 인스턴스 항목 추출을 위해 ec2 client 실행
        for ec2_region in regions:
            ec2_list = boto3.client('ec2', region_name=ec2_region)
            instances = ec2_list.describe_instances(Filters=[{'Name': 'instance-state-name', 'Values': ['running', 'stopped']}])

            # 리전 속 인스턴스 정보 출력
            for reservation in instances['Reservations']:
                for instance in reservation['Instances']:

                    # 인스턴스 이름 설정
                    for tag in instance['Tags']:
                        if tag['Key'] == 'Name':
                            instance_name = tag['Value']
                            break

                    # 인스턴스 정보 임시 저장
                    instance_type = instance['InstanceType']
                    instance_state = instance['State']['Name']
                    if instance_state == 'running':
                        launch_time = instance['LaunchTime'].replace(tzinfo=timezone.utc)
                        instance_time = current_time - launch_time
                    else:
                        stopped_time = re.findall('.*\((.*)\)', instance['StateTransitionReason'])[0][:-4]
                        instance_time = current_time - datetime.strptime(stopped_time, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                    days = instance_time.days
                    hours = instance_time.seconds // 3600
                    minutes = (instance_time.seconds % 3600) // 60

                    # 인스턴스 볼륨 아이디
                    for mapping in instance['BlockDeviceMappings']:
                        if mapping['DeviceName'] == '/dev/sda1':
                            volume_Id = mapping['Ebs']['VolumeId']

                    # 저장항목 변수화
                    instance_dsc = {'region':ec2_region, 'name':instance_name, 'type':instance_type, 'volume':volume_Id, 'time_days':days, 'time_hours':hours, 'time_minutes':minutes}

                    # 인스턴스 정보 리스트로 저장
                    if instance_state == 'running':
                        running_instances.append(instance_dsc)
                    else:
                        stopped_instances.append(instance_dsc)

        # 인스턴스 항목 내림차순 정렬
        sorted_running_instances = sorted(running_instances, key=lambda x: (x['time_days'], x['time_hours'], x['time_minutes']), reverse=True)
        sorted_stopped_instances = sorted(stopped_instances, key=lambda x: (x['time_days'], x['time_hours'], x['time_minutes']), reverse=True)

        return sorted_running_instances, sorted_stopped_instances
    except Exception as e:
        send_exception_message(head_message, "인스턴스 관리 실패\n", e)


# get volume items - 모든 리전의 볼륨을 조회한 후 리스트로 반환한다.
def get_volume_items(current_time, head_message, regions):
    try:
        orphaned_volumes = []

        # 리전 별로 volume 확인을 위해 clien 실행
        for volume_region in regions:
            volume_list = boto3.client('ec2', region_name=volume_region)
            volumes = volume_list.describe_volumes(Filters=[{'Name': 'status', 'Values': ['available']}])

            # 각 EBS 볼륨의 상태 확인
            for volume in volumes['Volumes']:
                volume_id = volume['VolumeId']
                size_gb = volume['Size']
                volume_type = volume['VolumeType']
                snapshot_id = volume['SnapshotId']
                created_time = current_time - volume['CreateTime'].replace(tzinfo=timezone.utc)
                orphaned_volumes.append({'region': volume_region, 'id':volume_id, 'type':volume_type, 'size':size_gb, 'snapshot':snapshot_id, 'time':created_time.days})

        sorted_orphaned_volumes = sorted(orphaned_volumes, key=lambda x: (x['time']), reverse=True)

        return sorted_orphaned_volumes
    except Exception as e:
        send_exception_message(head_message, "볼륨 관리 실패\n", e)


# create message - 슬랙으로 보낼 메세지를 생성한다.
def create_message(head_message, running_list, stopped_list, volume_list):
    try:
        message = head_message

        if len(running_list) > 0:
            message += (f"\n[Running EC2 Instances] ({len(running_list)})\n")
            for running_instance in running_list:
                meg = (f"{running_instance['region']} / {running_instance['name']} / {running_instance['type']} / {running_instance['volume']} ~ {running_instance['time_days']}일 {running_instance['time_hours']}시간 {running_instance['time_minutes']}분간")
                if running_instance['time_days'] == 0 or running_instance['time_days'] > 3:
                    message += (meg+" 실행 중 :large_green_circle:\n")
                else:
                    message += (meg+" 실행 중 :red_circle:\n")
        
        if len(stopped_list) > 0:
            message += (f"\n[Stopped EC2 Instances] ({len(stopped_list)})\n")
            for stopped_instance in stopped_list:
                meg = (f"{stopped_instance['region']} / {stopped_instance['name']} / {stopped_instance['type']} / {stopped_instance['volume']} ~ {stopped_instance['time_days']}일 {stopped_instance['time_hours']}시간 {stopped_instance['time_minutes']}분간")
                if stopped_instance['time_days'] < 7:
                    message += (meg+" 정지 중 :white_circle:\n")
                elif stopped_instance['time_days'] < 13:
                    message += (meg+" 정지 중 :large_yellow_circle:\n")
                else:
                    message += (meg+" 정지 중 :large_brown_circle:\n")
        
        if len(volume_list) > 0:
            message += (f"\n[Orphaned Volumes] ({len(volume_list)})\n")
            for volume in volume_list:
                message += (f"{volume['region']} / {volume['id']} / {volume['type']} / {volume['size']} / {volume['snapshot']} ~ {volume['time']}일 동안 존재 :warning:\n")

        return message
    except Exception as e:
        send_exception_message(head_message, "메세지 생성 오류\n", e)


# generate curl message - 슬랙으로 보낼 메세지 payload 형식을 결정한다.
def generate_curl_message(message):
    payload = {"text": message}
    return json.dumps(payload).encode("utf-8")


# post message - 주어진 URL에 JSON 형식의 데이터를 전송한다.
def post_message(url, data):
    req = urllib.request.Request(url)
    req.add_header("Content-Type", "application/json")
    return urllib.request.urlopen(req, data)


# send exception message - 오류로 인해 발생한 메세지를 슬랙으로 보내고 실행을 종료한다.
def send_exception_message(head_message, exception_message, e):
    message = (f"{head_message}{exception_message}{e}")

    data = generate_curl_message(message)
    response = post_message(SLACK_URL, data)
    sys.exit()


# lambda handler - 람다 함수를 실행한다.
def lambda_handler(event, context):
    current_time = datetime.now(timezone.utc)

    head_message = "Account: bigdata@kookmin.ac.kr\n"
    cur_time = (current_time + timedelta(hours=9)).strftime('%Y-%m-%d %H:%M')
    head_message += (cur_time+"\n")
    
    running_instances, stopped_instances, orphaned_volumes  = enable_instance_management(current_time, head_message)
    message = create_message(head_message, running_instances, stopped_instances, orphaned_volumes)
    
    data = generate_curl_message(message)
    response = post_message(SLACK_URL, data)
    
    return (f"{response.status}! 인스턴스 관리에 성공하였습니다. 총 인스턴스는 {len(running_instances)+len(stopped_instances)}개 입니다.")
    