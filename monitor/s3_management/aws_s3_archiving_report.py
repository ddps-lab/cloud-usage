import boto3, os
from botocore.exceptions import ClientError
import urllib.request, urllib.parse, json
from datetime import datetime, timedelta, timezone

SLACK_URL = os.environ['SLACK_DDPS']

try:
    DEADLINE_MONTHS = int(os.environ['MONTHS'])
except KeyError:
    DEADLINE_MONTHS = 6
DEADLINE = datetime.now() - timedelta(days=DEADLINE_MONTHS*30)


# get archiving bucket : 아카이브행 버킷 탐색
def get_archiving_bucket(pass_list):
    s3_client = boto3.client('s3')
    bucket_list = s3_client.list_buckets()
    archiving_list = []
    bucket_name_max = 0
    
    for bucket in bucket_list['Buckets']:
        bucket_name = bucket["Name"]
        
        # 아카이빙 목록에서 제외해야 하는 버킷이라면 패스함
        if bucket_name in pass_list:
            continue
        
        bucket_name_len = len(bucket_name)
        bucket_name_max = max(bucket_name_max, bucket_name_len)

        try:
            bucket_objects = s3_client.list_objects(Bucket=bucket_name)
        except (ClientError, NameError) as e:
            continue
        bucket_size = 0
        last_accessed_date = "N/A"
        archiving_bucket = True
        
        if 'Contents' in bucket_objects:
            last_accessed = []
            for content in bucket_objects['Contents']:
                last_accessed.append(content['LastModified'].strftime("%Y-%m-%d"))
            last_accessed_date = max(last_accessed)
            item_date = datetime.strptime(last_accessed_date, "%Y-%m-%d")
            if item_date > DEADLINE:
                archiving_bucket = False
            
            if archiving_bucket == True and last_accessed_date != "N/A":
                for content in bucket_objects['Contents']:
                    if content['StorageClass'] == "STANDARD":
                        bucket_size += content['Size']
            bucket_size = round(bucket_size/(1000000), 2) # MB 단위

        if archiving_bucket and last_accessed_date != "N/A":
            archiving_list.append([bucket_name, bucket_size, last_accessed_date]) 
  
    ordered_archiving_list = sorted(archiving_list, key=lambda x: x[1], reverse=True)
    return ordered_archiving_list, bucket_name_max
                

# created message : 탐색된 아카이브행 버킷을 메세지로 생성
def created_message(now_time, archiving_list, bucket_name_max):
    messages = []
    header = "*S3 Bucket List to be Archived* - [" + str(len(archiving_list)) + " buckets]\n"
    header += (now_time+"\n")

    if len(archiving_list) > 0:
        header += "* 금월 6일에 Glacier로 이동할 버킷 리스트입니다.\n* 해당 버킷이 Glacier로 이동하길 원하지 않으시면, 백업 혹은 새로 액세스해주시길 바랍니다.\n"
        header += "비고) 액세스 방법은 s3 버킷 내에 업로드, 삭제, 수정 등의 변화가 일어나야 하는 점을 유의해주십시오.\n"
    
        message = f'{"No":>2}. {"Bucket Name":{bucket_name_max+2}} {"Size":12} {"Last Modified"}'
        count = 1
        for item in archiving_list:
            if item[1] >= 1000:
                item[1] = str(round(item[1]/1000, 2)) + " GB"
            else:
                item[1] = str(item[1]) + " MB"
            message += f'\n{count:>2}. {item[0]:{bucket_name_max+2}} {item[1]:12} {item[2]}'
            count += 1
            if len(message) > 3800:
                messages.append(message)
                message = ""
        messages.append(message)
    else:
        header += "금월에 Glacier로 옮길 항목이 없습니다."
    return header, messages
    

# slack message : 생성한 메세지를 슬랙으로 전달
def slack_message(message, meg_type):
    if meg_type == True:
        payload = {"text": message}
    else:
        payload = {"text": f'```{message}```'}
    data = json.dumps(payload).encode("utf-8")
    
    req = urllib.request.Request(SLACK_URL)
    req.add_header("Content-Type", "application/json")
    return urllib.request.urlopen(req, data)


# lambda handler : 람다 실행
def lambda_handler(event, context):
    # 람다 환경변수로부터 패스해야 하는 버킷 리스트를 읽음
    pass_list = [item.strip() for item in os.environ['PASS_LIST'].split(',')]
    
    utc_time = datetime.now(timezone.utc)
    korea_time = (utc_time + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M")

    bucket_result_list, bucket_name_max = get_archiving_bucket(pass_list)
    header, messages = created_message(korea_time, bucket_result_list, bucket_name_max)
    
    response = slack_message(header, True)
    
    if not messages:
        return "There are no items to move to Glacier. Check the Slack message."
    else:
        for meg in messages:
            response = slack_message(meg, False)

    return "The Archive List was successfully sent in a Slack. Check the Slack message."