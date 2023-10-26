import boto3
from botocore.exceptions import ClientError
import urllib.request, urllib.parse, json, configparser
from datetime import datetime,  timedelta


# auto_archiving - 아카이브할 버킷 탐색 및 아카이브 진행
def auto_archiving(session, DEADLINE_MONTHS):
    s3_client = session.client('s3')
    bucket_list = s3_client.list_buckets()
    buckets = bucket_list['Buckets']

    archiving_list = []
    error_list = []
    deadline = datetime.now() - timedelta(days=DEADLINE_MONTHS*30)

    for bucket in buckets:
        bucket_name = bucket['Name']
        standard_size = 0
        last_accessed_date = "N/A"
        archiving_bucket = True

        try:
            bucket_objects = s3_client.list_objects_v2(Bucket=bucket_name)
        except (ClientError, NameError) as e:
            continue

        if 'Contents' in bucket_objects:
            last_accessed = []
            for content in bucket_objects['Contents']:
                last_accessed.append(content['LastModified'].strftime("%Y-%m-%d"))
            last_accessed_date = max(last_accessed)
            item_date = datetime.strptime(last_accessed_date, "%Y-%m-%d")
            if item_date > deadline:
                archiving_bucket = False
        
            if archiving_bucket == True and last_accessed_date != "N/A":
                error_bucket = []
                for content in bucket_objects['Contents']:
                    if content['StorageClass'] == 'STANDARD':
                        standard_size += content['Size']
                        try:
                            s3_client.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': content['Key']}, Key=content['Key'], StorageClass='GLACIER')
                        except ClientError as e:
                            error_bucket.append([content['Key']])
                if len(error_bucket) > 0:
                    error_list.append([bucket_name, error_bucket])
                archiving_list.append([bucket_name, standard_size])
                
    if len(archiving_list) > 0:
        ordered_archiving_list = sorted(archiving_list, key=lambda x: x[1], reverse=True)
    else:
        ordered_archiving_list = []
    return ordered_archiving_list, error_list
                        

# created message - 아카이브 결과를 메세지로 생성
def created_message(now_time, archiving_list, error_list):
    message = f'*s3 archiving management* ({now_time})'
    if len(archiving_list) > 0:
        count = 1
        message += f"\n{len(archiving_list)}개의 버킷을 Glacier로 옮겼습니다.\n"
        for bucket in archiving_list:
            if bucket[1] >= 1000000000:
                size = round(bucket[1]/1000000000, 2)
                message += f"\n{count}.  {bucket[0]}    {size}GB"
            elif bucket[1] >= 1000000:
                size = round(bucket[1]/1000000, 2)
                message += f"\n{count}.  {bucket[0]}    {size}MB"
            else:
                message += f"\n{count}.  {bucket[0]}    {size}"
            count += 1
    else:
        message += "\nGlacier로 옮길 항목이 없습니다.\n"
    if len(error_list) > 0:
        message += f"\n---\n{len(error_list)}개의 버킷에 5GB가 넘는 항목이 존재합니다.\n"
        for bucket in error_list:
            message += f"\n버킷 이름 : {bucket[0]}\n목록 :"
            for key in bucket[1]:
                message += f"\n- {key}"
    return message


# slack message : 생성된 메세지를 슬랙으로 전달
def slack_message(message, url):
    payload = {"text": message}
    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(url)
    req.add_header("Content-Type", "application/json")
    return urllib.request.urlopen(req, data)


if __name__ == '__main__':
    aws_profile = 'ddps-usage'
    session = boto3.Session(profile_name=aws_profile)

    config = configparser.ConfigParser()
    config.read('/home/ubuntu/config.ini')
    
    DEADLINE_MONTHS = int(config.get('s3_setting', 'DEADLINE_MONTHS'))
    SLACK_URL = config.get('s3_setting', 'SLACK_URL')

    utc_time = datetime.utcnow()
    korea_time = (utc_time + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M")

    archiving_list, error_list = auto_archiving(session, DEADLINE_MONTHS)
    message = created_message(korea_time, archiving_list, error_list)
    response = slack_message(message, SLACK_URL)