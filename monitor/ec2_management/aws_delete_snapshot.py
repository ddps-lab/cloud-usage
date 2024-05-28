import boto3
import urllib.request, json, os
from datetime import datetime, timedelta, timezone
from slack_msg_sender import send_slack_message

SLACK_URL = os.environ['SLACK_URL']
    
sts_client = boto3.client('sts')
response = sts_client.get_caller_identity()
ACCOUNT_ID = response['Account']


# remove_ami : available 상태가 아닌 모든 AMI 삭제
def remove_ami(ebs):
    ami_count = 0
    ami_dic = {"available":[], "disabled":[]}
    all_ami = ebs.describe_images(
        Owners=[ACCOUNT_ID], 
        Filters=[{'Name': 'is-public','Values': ['false']}],
        IncludeDeprecated=True,
        IncludeDisabled=True,
    )
    for ami in all_ami.get("Images"):
        storage = ami.get("BlockDeviceMappings")[0].get("Ebs")
        if ami.get("State") != "available":
            try:
                ebs.deregister_image(ImageId=ami.get("ImageId"))
                ami_dic["disabled"].append(storage.get("SnapshotId"))
                ami_count += 1
            except Exception as e:
                send_slack_message(f"AMI 삭제 실패 :\n{e}")
        else:
            ami_dic["available"].append(storage.get("SnapshotId")) 

    return ami_count, ami_dic


# remove_snapshot : tag 를 부여하지 않은 모든 스냅샷 삭제
def remove_snapshot(ebs, ami_dic):
    snapshot_count = 0
    snapshots = ebs.describe_snapshots(OwnerIds=[ACCOUNT_ID])
    # snapshot list in region
    for snapshot in snapshots["Snapshots"]:
        try:
            if snapshot["SnapshotId"] not in ami_dic["available"] or snapshot["SnapshotId"] in ami_dic["disabled"]:
                ebs.delete_snapshot(SnapshotId=snapshot["SnapshotId"])
                snapshot_count += 1
            if snapshot.get("Tags"):
                continue
        except Exception as e:
                send_slack_message(f"스냅샷 삭제 실패 :\n{e}")
    
    return snapshot_count


# created message : 슬랙에 알릴 내용을 메세지로 생성
def created_message(result):
    message = ""
    for region in result:
        item = result[region]
        if item.get("ami_count") > 0 or item.get("snapshot_count") > 0:
            message += f"{region}\n"
        if item.get("ami_count") > 0:
            message += f"> AMI {item.get("ami_count")}개 삭제 \n"
        if item.get("snapshot_count") > 0:
            message += f"> 스냅샷 {item.get("snapshot_count")}개 삭제 \n"
    
    if message == "":
        message = "> There is no snapshot list to delete."
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
    # Record code execution time.
    utc_time = datetime.now(timezone.utc)
    korea_time = (utc_time + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M")

    # Get a region list from ec2.
    client = boto3.client('ec2')
    regions = [ region['RegionName'] for region in client.describe_regions()['Regions'] ]

    # Set a list value for recording snapshot list by region.
    result = {}

    # Get snapshot lists by get_snapshot_list function.
    for region in regions:
        ebs = boto3.client('ec2', region_name=region)
        ami_count, ami_list = remove_ami(ebs)
        snapshot_count = remove_snapshot(ebs, ami_list)
        if ami_count > 0 or snapshot_count > 0:
            result[region] = {"ami_count":ami_count, "snapshot_count":snapshot_count}
        
    # Write a message to send to slack
    head_message = f"*snapshot management* ({korea_time})\n"
    message = created_message(result)
    
    # Send a message to slack
    slack_message(head_message)
    slack_message(message)
    
    return "The snapshot is deleted succesfully. Check the Slack message."