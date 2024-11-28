import boto3, re, os
import urllib.request, urllib.parse, json
from datetime import datetime, timezone, timedelta
from slack_msg_sender import send_slack_message


SLACK_URL = os.environ['SLACK_DDPS']

# get instance items : 모든 리전의 인스턴스 탐색 후 리스트 반환
def get_instance_items(regions):
    try:
        running_instances = []
        stopped_instances = []

        # 리전에 존재하는 모든 인스턴스 탐색
        for ec2_region in regions:
            ec2_list = boto3.client('ec2', region_name=ec2_region)
            instances_data = ec2_list.describe_instances(Filters=[{'Name': 'instance-state-name', 'Values': ['running', 'stopped']}]).get('Reservations')
            
            if not instances_data:
                continue
            
            current_time = datetime.now(timezone.utc)

            # 한 리전의 인스턴스 정보 추출
            for instances in instances_data:
                if instances.get('Instances') is None:
                    continue

                for instance in instances.get('Instances'):
                    # 인스턴스 탐색
                    key_name = instance.get('KeyName')

                    instance_info = instance.get('InstanceId')
                    if instance.get('Tags') is not None:
                        for tag in instance.get('Tags'):
                                if tag.get('Key') == 'Name':
                                    instance_info = tag.get('Value')
                                    break
                        
                    instance_type = instance.get('InstanceType')
                    instance_state = instance['State']['Name']
                    if instance_state == 'running':
                        launch_time = instance['LaunchTime'].replace(tzinfo=timezone.utc)
                        instance_time = current_time - launch_time
                    else:
                        stopped_time = re.findall(r'.*\((.*)\)', instance['StateTransitionReason'])[0][:-4]
                        instance_time = current_time - datetime.strptime(stopped_time, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                    days = instance_time.days
                    hours = instance_time.seconds // 3600
                    minutes = (instance_time.seconds % 3600) // 60

                    # 인스턴스의 볼륨ID 확인
                    for mapping in instance['BlockDeviceMappings']:
                        volume_id = mapping['Ebs']['VolumeId']

                    # 인스턴스 저장
                    instance_dsc = {'region':ec2_region, 'key_name':key_name, 'info':instance_info, 'type':instance_type, 'volume':volume_id, 'time_days':days, 'time_hours':hours, 'time_minutes':minutes}
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
def get_volume_items(regions):
    try:
        orphaned_volumes = []

        # 리전 별로 volume 탐색
        for volume_region in regions:
            volume_list = boto3.client('ec2', region_name=volume_region)
            volumes = volume_list.describe_volumes(Filters=[{'Name': 'status', 'Values': ['available']}])
            
            current_time = datetime.now(timezone.utc)
            # 하나의 EBS 볼륨 확인
            if volumes.get('Volumes') is not None:
                for volume in volumes.get('Volumes'):
                    volume_id = volume.get('VolumeId')
                    size_gb = volume.get('Size')
                    volume_type = volume.get('VolumeType')
                    snapshot_id = volume.get('SnapshotId')
                    created_time = current_time - volume.get('CreateTime').replace(tzinfo=timezone.utc)
                    
                    # check the callisto volume
                    callisto_volume = False
                    if volume.get('Tags') is not None:
                        for key in volume['Tags']:
                            if "kubernetes.io" in key.get('Key'):
                                callisto_volume = True
                                break
                        if callisto_volume and created_time.days <= 14:
                            continue
                    orphaned_volumes.append({'region': volume_region, 'id':volume_id, 'type':volume_type, 'size':size_gb, 'snapshot':snapshot_id, 'time':created_time.days, 'callisto':callisto_volume})

        sorted_orphaned_volumes = sorted(orphaned_volumes, key=lambda x: (x['time']), reverse=True)
        return sorted_orphaned_volumes
    except Exception as e:
        print(f"볼륨 조회 실패\n{e}")


# created message : 인스턴스 및 볼륨 리스트를 메세지로 생성
def created_message(head_message, running_list, stopped_list, volume_list):
    try:
        message = head_message

        if len(running_list) > 0:
            message += (f"\n[Running EC2 Instances] ({len(running_list)})\n")
            for running_instance in running_list:
                meg = (f"{running_instance['region']} / {running_instance['info']}({running_instance['type']}) / {running_instance['key_name']} / {running_instance['volume']} ~ {running_instance['time_days']}일 {running_instance['time_hours']}시간 {running_instance['time_minutes']}분간")
                if running_instance['time_days'] == 0 or running_instance['time_days'] > 3:
                    message += (meg+" 실행 중 :large_green_circle:\n")
                else:
                    message += (meg+" 실행 중 :red_circle:\n")
        
        if len(stopped_list) > 0:
            message += (f"\n[Stopped EC2 Instances] ({len(stopped_list)})\n")
            for stopped_instance in stopped_list:
                meg = (f"{stopped_instance['region']} / {stopped_instance['info']}({stopped_instance['type']}) / {stopped_instance['key_name']} / {stopped_instance['volume']} ~ {stopped_instance['time_days']}일 {stopped_instance['time_hours']}시간 {stopped_instance['time_minutes']}분간")
                if stopped_instance['time_days'] < 7:
                    message += (meg+" 정지 중 :white_circle:\n")
                elif stopped_instance['time_days'] < 13:
                    message += (meg+" 정지 중 :large_yellow_circle:\n")
                else:
                    message += (meg+" 정지 중 :large_brown_circle:\n")

        if len(volume_list) > 0:
            message += (f"\n[Orphaned Volumes] ({len(volume_list)})\n")
            for volume in volume_list:
                message += (f"{volume['region']} / {volume['id']} / {volume['type']} / {volume['size']} / {volume['snapshot']} ~ {volume['time']}일 동안 존재 ")
                if volume['callisto']:
                    message += (":comet:\n")
                else:
                    message += (":warning:\n")

        if len(message) == len(head_message):
            message += "No instances have been running or stopped."

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
    utc_time = datetime.now(timezone.utc)
    korea_time = (utc_time + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M")

    head_message = f"Account: test@ddps.com\n"
    head_message += (korea_time+"\n")

    running_instances, stopped_instances, orphaned_volumes = 0, 0, 0
    
    try:
        # regions 검색용 Boto3 EC2 클라이언트 생성
        ec2_client = boto3.client('ec2')
        regions = [ region['RegionName'] for region in ec2_client.describe_regions()['Regions'] ]

        running_instances, stopped_instances = get_instance_items(regions)
        orphaned_volumes = get_volume_items(regions)
    except Exception as e:
        send_slack_message(f"인스턴스 관리가 정상적으로 이루어지지 않았습니다.\n{e}")
    try:
        message = created_message(head_message, running_instances, stopped_instances, orphaned_volumes)
        response = slack_message(message)
        return "The Instance List was successfully sent in a Slack. Check the Slack message."
    except Exception as e:
        send_slack_message(f"메세지 생성이 원활하게 이루어지지 않았습니다.\n{e}")
        return "This instance management was failed. Check the Code or instances in aws."