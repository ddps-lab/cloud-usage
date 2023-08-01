import boto3
from botocore.exceptions import ClientError
import urllib.request, urllib.parse, json
from datetime import datetime, timezone, timedelta
import os

SLACK_URL = os.environ['SLACK_URL']

try:
    DEADLINE_MONTHS = int(os.environ['MONTHS'])
except KeyError:
    DEADLINE_MONTHS = 6
deadline = datetime.now() - timedelta(days=DEADLINE_MONTHS*30)


# s3 버킷을 불러오고 리스트로 목록을 만듭니다.
def get_s3_bucket(version):
    s3_client = boto3.client('s3')
    bucket_list = s3_client.list_buckets()
    bucket_result_list = []
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
        go_to_glacier = True
        
        if 'Contents' in bucket_objects:
            last_accessed = [objects['LastModified'].strftime("%Y-%m-%d") for objects in bucket_objects['Contents'] if 'LastModified' in objects]
            if last_accessed:
                last_accessed_date = max(last_accessed)
                item_date = datetime.strptime(last_accessed_date, "%Y-%m-%d")
                if item_date > deadline:
                    go_to_glacier = False
                
                
            objects_sizes = [objects['Size'] for objects in bucket_objects['Contents'] if objects.get("Size") is not None]
            if objects_sizes:
                for obj in objects_sizes:
                    bucket_size += obj
            bucket_size = round(bucket_size/(1000*1000), 2) # MB 단위


        if version == 0:
            result = [bucket_name, bucket_size, last_accessed_date]
            bucket_result_list.append(result)
        else:
            if version == 1:
                if go_to_glacier == True:
                    result = [bucket_name, bucket_size, last_accessed_date]
                    bucket_result_list.append(result) 
            elif version == 2:
                if go_to_glacier == True:
                    result = [bucket_name][0]
                    if isinstance(result, list):
                        result = result[0]
                    bucket_result_list.append(result)    
            
    
    if version == 2:
        return bucket_result_list
    else:
        orderer_bucket_result_list = sorted(bucket_result_list, key=lambda x: x[1], reverse=True)
        return orderer_bucket_result_list, bucket_name_max
                

# 콘솔 또는 슬랙으로 보낼 메세지를 생성합니다.
def generate_mm_message(bucket_result_list, bucket_name_max, version):
    messages = []
    header = "S3 Bucket Report"
    if version == 0:
        header = "S3 Bucket List - [" + str(len(bucket_result_list)) + " buckets]\n"
    elif version == 1:
        header = "S3 Bucket List to be Archived - [" + str(len(bucket_result_list)) + " buckets]\n"
    crrent_time = datetime.now(timezone(timedelta(hours=9))).strftime('%Y-%m-%d %H:%M')
    header += (crrent_time+"\n")
    if version == 1:
        header += "* 5일 뒤에 Glacier로 이동할 버킷 리스트입니다.\n* 해당 버킷이 Glacier로 이동하길 원하지 않으시면, 백업 혹은 새로 액세스해주시길 바랍니다.\n"
        header += "비고) 액세스 방법은 s3 버킷 내에 업로드, 삭제, 수정 등의 변화가 일어나야 하는 점을 유의해주십시오.\n"
    
    message = f'{"No":>2}. {"Bucket Name":{bucket_name_max+2}} {"Size":12} {"Last Modified"}'
    count = 1
    for item in bucket_result_list:
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
    

# 슬랙으로 보낼 메세지 payload 형식을 결정합니다.
def generate_curl_message(message, meg_type):
    if meg_type == 'h':
        payload = {"text": message}
    elif meg_type == 'm':
        payload = {"text": f'```{message}```'}
    return json.dumps(payload).encode("utf-8")

# 주어진 URL에 JSON 형식의 데이터를 전송합니다.
def post_message(url, data):
    req = urllib.request.Request(url)
    req.add_header("Content-Type", "application/json")
    return urllib.request.urlopen(req, data)


# lambda_handler : 받아온 event에 따라 version을 달리합니다.
def lambda_handler(event, context):
    url = SLACK_URL
    version = 0

    event_bridge_value = event['resources'][0]
    if event_bridge_value == 'arn:aws:events:ap-northeast-2:741926482963:rule/usage-s3-1st_Month':
        version = 0
    elif event_bridge_value == 'arn:aws:events:ap-northeast-2:741926482963:rule/usage-s3-1st_EvenMonth':
        version = 1 
    elif event_bridge_value == 'arn:aws:events:ap-northeast-2:741926482963:rule/usage-s3-6th_EvenMonth':
        version = 1
    elif event_bridge_value == 'arn:aws:events:ap-northeast-2:741926482963:rule/usage-go_to_Glacier':
        version = 2

    if version == 2:
        bucket_result_list = get_s3_bucket(version)
        target_lambda_arn = 'arn:aws:lambda:us-west-1:741926482963:function:yrkim-lambda-test'
    
        lambda_client = boto3.client('lambda')
        response = lambda_client.invoke(
            FunctionName=target_lambda_arn,
            Payload=json.dumps(bucket_result_list)
        )
        return "successfully", len(bucket_result_list)
    else:
        bucket_result_list, bucket_name_max = get_s3_bucket(version)
        header, messages = generate_mm_message(bucket_result_list, bucket_name_max, version)
    
        data = generate_curl_message(header, 'h')
        response = post_message(url, data)
    
        for meg in messages:
            data = generate_curl_message(meg, 'm')
            response = post_message(url, data)
        
        return response.status
        #return header, messages, len(bucket_result_list)