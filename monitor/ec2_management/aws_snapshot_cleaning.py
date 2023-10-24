import boto3
import urllib.request, json, os
import datetime, pytz


SLACK_URL = os.environ['SLACK_URL']

# snapshot management : 스냅샷 목록을 불러온 후 불필요 조건에 만족한 스냅샷 삭제
def snapshot_management(account_id):
    ec2_client = boto3.client('ec2')
    regions = [ region['RegionName'] for region in ec2_client.describe_regions()['Regions'] ]
    result_snapshot = {}

    for region in regions:
        ebs = boto3.client('ec2', region_name=region)
        snapshots = ebs.describe_snapshots(OwnerIds=[account_id])

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
        amies = ebs.describe_images(Owners=[account_id], Filters=[{'Name': 'is-public','Values': ['false']}])
        ami_list = {}
        for ami in amies['Images']:
            block_device = ami['BlockDeviceMappings']
            snapshot = block_device[0]['Ebs']
            snapshot_id = snapshot['SnapshotId']
            ami_list[ami['ImageId']] = snapshot_id

        # snapshot checking in the volume
        for vID, snapID in volume_list.items():
            if snapID in snapshot_list:
                del snapshot_list[snapID]
        
        # snapshot checking in the ami
        for amiID, snapID in ami_list.items():
            if snapID in snapshot_list:
                del snapshot_list[snapID]
        result_snapshot[region] = 0
        
        # delete snapshot
        for snapID in snapshot_list:
            ebs.delete_snapshot(SnapshotId=snapID)
            result_snapshot[region] += 1
    return result_snapshot
    

# created message : 슬랙에 알릴 내용을 메세지로 생성
def created_message(head_message, result_snapshot):
    message = head_message + '\n'
    for region, count in result_snapshot.items():
        if count > 0:
            message += f"> {region}   :   {count}개의 스냅샷 제거\n"
    return message


# slack message : 생성한 메세지를 슬랙으로 전달
def slack_message(message, url):
    payload = {"text": message}
    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(url)
    req.add_header("Content-Type", "application/json")
    return urllib.request.urlopen(req, data)


# lambda handler : 람다 실행
def lambda_handler(event, context):
    url = SLACK_URL
    
    utc_time = datetime.datetime.utcnow()
    korea_timezone = pytz.timezone('Asia/Seoul')
    korea_time = (utc_time.replace(tzinfo=pytz.utc).astimezone(korea_timezone)).strftime("%Y-%m-%d %H:%M:%S")

    head_message = f"*snapshot management* ({korea_time})\n"
    
    sts_client = boto3.client('sts')
    response = sts_client.get_caller_identity()
    account_id = response['Account']

    result_snapshot = snapshot_management(account_id)
    message = created_message(head_message, result_snapshot)
    slack_message(message, url)
    
    return "The snapshot is deleted succesfully. Check the Slack message."