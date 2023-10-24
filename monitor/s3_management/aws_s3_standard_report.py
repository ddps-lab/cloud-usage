import boto3
from botocore.exceptions import ClientError
import urllib.request, urllib.parse, json
from datetime import datetime, timezone, timedelta
import os

SLACK_URL = os.environ['SLACK_URL']


# get s3 bucket : s3 버킷 중 standard class 만 리스트 생성
def get_s3_bucket():
    s3_client = boto3.client('s3')
    bucket_list = s3_client.list_buckets()
    standard_list = []
    bucket_name_max = 0
    
    for bucket in bucket_list['Buckets']:
        bucket_name = bucket["Name"]
        bucket_name_len = len(bucket_name)
        bucket_name_max = max(bucket_name_max, bucket_name_len)

        try:
            bucket_objects = s3_client.list_objects(Bucket=bucket_name)
        except (ClientError, NameError) as e:
            continue
        bucket_size = 0
        last_accessed_date = "N/A"
        bucket_class = "STANDARD"
        
        if 'Contents' in bucket_objects:
            last_accessed = []
            for content in bucket_objects['Contents']:
                last_accessed.append(content['LastModified'].strftime("%Y-%m-%d"))
            last_accessed_date = max(last_accessed)

            for content in bucket_objects['Contents']:
                if content['StorageClass'] == "STANDARD":
                    bucket_size += content['Size']
            bucket_size = round(bucket_size/(1000000), 2) # MB 단위

        if bucket_size == 0:
            bucket_class = "GLACIER"
        
        if bucket_class == "STANDARD":
            standard_list.append([bucket_name, bucket_size, last_accessed_date])
    ordered_standard_list = sorted(standard_list, key=lambda x: x[1], reverse=True)
    return ordered_standard_list, bucket_name_max
               

# created message : standard bucket을 메세지로 생성
def created_message(standard_list, bucket_name_max):
    messages = []
    header = "S3 Bucket List - [" + str(len(standard_list)) + " buckets]\n"
    crrent_time = datetime.now(timezone(timedelta(hours=9))).strftime('%Y-%m-%d %H:%M')
    header += (crrent_time+"\n")
   
    message = f'{"No":>2}. {"Bucket Name":{bucket_name_max+2}} {"Size":12} {"Last Modified"}'
    count = 1
    for item in standard_list:
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
    return header, messages
    

# slack message : 생성한 메세지를 슬랙으로 전달
def slack_message(message, meg_type, url):
    if meg_type == True:
        payload = {"text": message}
    else:
        payload = {"text": f'```{message}```'}
    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(url)
    req.add_header("Content-Type", "application/json")
    return urllib.request.urlopen(req, data)


# lambda_handler : 람다 실행
def lambda_handler(event, context):
    url = SLACK_URL
		
    bucket_standard_list, bucket_name_max = get_s3_bucket()
    header, messages = created_message(bucket_standard_list, bucket_name_max)
    
    response = slack_message(header, True, url)
    
    for meg in messages:
        response = slack_message(meg,False, url)
        
    return "All bucket list of s3 was sent in a slack. Check the Slack message."