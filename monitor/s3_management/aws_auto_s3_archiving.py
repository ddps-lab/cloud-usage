import boto3
from botocore.exceptions import ClientError
import urllib.request, urllib.parse, json
from datetime import datetime, timezone, timedelta


# auto_archiving - 아카이브할 버킷 탐색 및 아카이브 진행
def auto_archiving(aws_access_key_id, aws_secret_access_key, DEADLINE_MONTHS):
    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
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
                error_list.append([bucket_name, error_bucket])
                archiving_list.append([bucket_name, standard_size])
    ordered_archiving_list = sorted(archiving_list, key=lambda x: x[2], reverse=True)
    return ordered_archiving_list, error_list
                        

# created message - 아카이브 결과를 메세지로 생성
def created_message(archiving_list, error_list):
    message = datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M")
    if len(archiving_list) > 0:
        count = 1
        message += f"\n{len(archiving_list)}개의 버킷을 Glacier로 옮겼습니다.\n"
        for bucket in archiving_list:
            if bucket[1] >= 1000:
                size = round(bucket[1]/1000, 2)
                message += f"\n{count}.  {bucket[0]}    {size}GB"
            else:
                message += f"\n{count}.  {bucket[0]}    {bucket[1]}MB"
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
    # AWS 인증 정보 - 이곳에 사용자 정보를 반드시 입력하세요.
    aws_access_key_id = ''
    aws_secret_access_key = ''

    # 기간 설정 (ex : 6 개월) - 기간을 설정해주세요. (기본값 : 6)
    DEADLINE_MONTHS = 6

    # slack url - slack url을 입력해주세요. (기본값 : ddps labs)
    url = ''

    # 아래 코드는 절대 건들지 마세요.
    print("S3 버킷을 관리 중입니다. 완료될 때까지 실행을 중지하지 마십시오.")
    archiving_list, error_list = auto_archiving(aws_access_key_id, aws_secret_access_key, DEADLINE_MONTHS)
    message = created_message(archiving_list, error_list)
    response = slack_message(message, url)
    print("작업이 종료되었습니다. 사용한 EC2 및 AMI는 반드시 삭제해주십시오.")