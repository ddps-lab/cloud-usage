import boto3, re, os
import urllib.request, urllib.parse, json
from datetime import datetime, timezone, timedelta
from slack_msg_sender import send_slack_message


SLACK_URL = os.environ['SLACK_URL']

# instance management : 모든 리전의 인스턴스와 볼륨 탐색 및 리스트 반환
def instance_management(current_time):
    try:
        # regions 검색용 Boto3 EC2 클라이언트 생성
        ec2_client = boto3.client('ec2')
        regions = [ region['RegionName'] for region in ec2_client.describe_regions()['Regions'] ]

        running_instances, stopped_instances = get_instance_items(current_time, regions)
        volume_list = get_volume_items(current_time, regions)

        return running_instances, stopped_instances, volume_list
    except Exception as e:
        send_slack_message(f"인스턴스와 볼륨 조회 실패.\n{e}")


# get instance items : 모든 리전의 인스턴스 탐색 후 리스트 반환
def get_instance_items(current_time, regions):
    try:
        running_instances = []
        stopped_instances = []

        # 리전에 존재하는 모든 인스턴스 탐색
        for ec2_region in regions:
            ec2_list = boto3.client('ec2', region_name=ec2_region)
            instances = ec2_list.describe_instances(Filters=[{'Name': 'instance-state-name', 'Values': ['running', 'stopped']}])

            # 한 리전의 인스턴스 정보 추출
            for reservation in instances['Reservations']:
                for instance in reservation['Instances']:
                    
                    # 인스턴스 탐색
                    try:
                        for tag in instance['Tags']:
                            if tag['Key'] == 'Name':
                                instance_info = tag['Value']
                                break
                    except Exception as e:
                        instance_info = instance['InstanceId']
                    instance_type = instance['InstanceType']
                    instance_state = instance['State']['Name']
                    if instance_state == 'running':
                        launch_time = instance['LaunchTime'].replace(tzinfo=timezone.utc)
                        instance_time = current_time - launch_time
                    else:
                        stopped_time = re.findall('.*\((.*)\)', instance['StateTransitionReason'])[0][:-4]
                        instance_time = current_time - datetime.strptime(stopped_time, '%Y-%m-%d %H:%M').replace(tzinfo=timezone.utc)
                    days = instance_time.days
                    hours = instance_time.seconds // 3600
                    minutes = (instance_time.seconds % 3600) // 60

                    # 인스턴스의 볼륨ID 확인
                    for mapping in instance['BlockDeviceMappings']:
                        if mapping['DeviceName'] == '/dev/sda1':
                            volume_id = mapping['Ebs']['VolumeId']

                    # 인스턴스 저장
                    instance_dsc = {'region':ec2_region, 'info':instance_info, 'type':instance_type, 'volume':volume_id, 'time_days':days, 'time_hours':hours, 'time_minutes':minutes}
                    if instance_state == 'running':
                        running_instances.append(instance_dsc)
                    else:
                        stopped_instances.append(instance_dsc)

        # 인스턴스 항목 내림차순 정렬
        sorted_running_instances = sorted(running_instances, key=lambda x: (x['time_days'], x['time_hours'], x['time_minutes']), reverse=True)
        sorted_stopped_instances = sorted(stopped_instances, key=lambda x: (x['time_days'], x['time_hours'], x['time_minutes']), reverse=True)

        return sorted_running_instances, sorted_stopped_instances
    except Exception as e:
        send_slack_message(f"인스턴스 조회 실패\n{e}")


# get volume items : 모든 리전의 볼륨 탐색 후 리스트 반환
def get_volume_items(current_time, regions):
    try:
        orphaned_volumes = []

        # 리전 별로 volume 탐색
        for volume_region in regions:
            volume_list = boto3.client('ec2', region_name=volume_region)
            volumes = volume_list.describe_volumes(Filters=[{'Name': 'status', 'Values': ['available']}])

            # 하나의 EBS 볼륨 확인
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
        send_slack_message(f"볼륨 조회 실패\n{e}")


# created message : 인스턴스 및 볼륨 리스트를 메세지로 생성
def created_message(head_message, running_list, stopped_list, volume_list):
    try:
        message = head_message

        if len(running_list) > 0:
            message += (f"\n[Running EC2 Instances] ({len(running_list)})\n")
            for running_instance in running_list:
                meg = (f"{running_instance['region']} / {running_instance['info']} / {running_instance['type']} / {running_instance['volume']} ~ {running_instance['time_days']}일 {running_instance['time_hours']}시간 {running_instance['time_minutes']}분간")
                if running_instance['time_days'] == 0 or running_instance['time_days'] > 3:
                    message += (meg+" 실행 중 :large_green_circle:\n")
                else:
                    message += (meg+" 실행 중 :red_circle:\n")
        
        if len(stopped_list) > 0:
            message += (f"\n[Stopped EC2 Instances] ({len(stopped_list)})\n")
            for stopped_instance in stopped_list:
                meg = (f"{stopped_instance['region']} / {stopped_instance['info']} / {stopped_instance['type']} / {stopped_instance['volume']} ~ {stopped_instance['time_days']}일 {stopped_instance['time_hours']}시간 {stopped_instance['time_minutes']}분간")
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
        send_slack_message(f"메세지 생성 실패\n{e}")


# slack message : 생성한 메세지를 슬랙으로 전달
def slack_message(message):
    payload = {"text": message}
    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(SLACK_URL)
    req.add_header("Content-Type", "application/json")
    return urllib.request.urlopen(req, data)


# lambda handler : 람다 실행
def lambda_handler(event, context):
    utc_time = datetime.utcnow()
    korea_time = (utc_time + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M")

    head_message = f"Account: {os.environ['EMAIL']}\n"
    head_message += (korea_time+"\n")

    try:
        running_instances, stopped_instances, orphaned_volumes  = instance_management(korea_time)
        message = created_message(head_message, running_instances, stopped_instances, orphaned_volumes)
        response = slack_message(message)
        return "The Instance List was successfully sent in a Slack. Check the Slack message."
    except Exception as e:
        send_slack_message(f"인스턴스 관리가 정상적으로 이루어지지 않았습니다.\n{e}")
        return "This instance management was failed. Check the Code or instances in aws."