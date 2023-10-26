import boto3
import urllib.request, json, os
from datetime import datetime, timedelta
from slack_msg_sender import send_slack_message


SLACK_URL = os.environ['SLACK_URL']
    
sts_client = boto3.client('sts')
response = sts_client.get_caller_identity()
ACCOUNT_ID = response['Account']


# snapshot management : 스냅샷 목록을 불러온 후 불필요 조건에 만족한 스냅샷 삭제
def snapshot_management():
    ec2_client = boto3.client('ec2')
    regions = [ region['RegionName'] for region in ec2_client.describe_regions()['Regions'] ]
    result_snapshot = {}

    for region in regions:
        ebs = boto3.client('ec2', region_name=region)
        snapshots = ebs.describe_snapshots(OwnerIds=[ACCOUNT_ID])

        # snapshot list in region
        snapshot_list = {}
        for snapshot in snapshots['Snapshots']:
            try:
                if snapshot['Tags']:
                    continue
            except KeyError as e:
                snapshot_list[snapshot['SnapshotId']] = snapshot['VolumeId']

        # snapshot list in volume
        volumes = ebs.describe_volumes(Filters=[{'Name': 'status', 'Values': ['in-use']}])
        volume_list = {}
        for volume in volumes['Volumes']:
            volume_list[volume['VolumeId']] = volume['SnapshotId']

        # snapshot list in ami
        amies = ebs.describe_images(Owners=[ACCOUNT_ID], Filters=[{'Name': 'is-public','Values': ['false']}])
        ami_list = {}
        for ami in amies['Images']:
            block_device = ami['BlockDeviceMappings']
            snapshot = block_device[0]['Ebs']
            snapshot_id = snapshot['SnapshotId']
            ami_list[ami['ImageId']] = snapshot_id

        # snapshot checking in the volume
        for volume_id, snapshot_id in volume_list.items():
            if snapshot_id in snapshot_list:
                del snapshot_list[snapshot_id]
        
        # snapshot checking in the ami
        for ami_id, snapshot_id in ami_list.items():
            if snapshot_id in snapshot_list:
                del snapshot_list[snapshot_id]
        
        # delete snapshot
        result_snapshot[region] = 0
        for snapshot_id in snapshot_list:
            try:
                ebs.delete_snapshot(SnapshotId=snapshot_id)
                result_snapshot[region] += 1
            except Exception as e:
                send_slack_message(f"스냅샷 삭제 실패\n{e}")
    return result_snapshot
    

# created message : 슬랙에 알릴 내용을 메세지로 생성
def created_message(head_message, result_snapshot):
    message = head_message + '\n'
    for region, count in result_snapshot.items():
        if count > 0:
            message += f"> {region}   :   {count}개의 스냅샷 제거\n"
    return message


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

    head_message = f"*snapshot management* ({korea_time})\n"

    result_snapshot = snapshot_management()
    message = created_message(head_message, result_snapshot)
    slack_message(message)
    
    return "The snapshot is deleted succesfully. Check the Slack message."