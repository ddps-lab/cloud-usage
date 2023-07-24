import boto3
import urllib.request, urllib.parse, json
from datetime import datetime, timezone, timedelta
import os
SLACK_URL = os.environ['SLACK_URL']

def get_s3_bucket():
    s3_client = boto3.client('s3')
    bucket_list = s3_client.list_buckets()
    bucket_result_list = []
    bucket_name_max = 0
    
    for bucket in bucket_list['Buckets']:
        bucket_name = bucket["Name"]
        bucket_name_len = len(bucket_name)
        bucket_name_max = max(bucket_name_max, bucket_name_len)
        if bucket_name.startswith(("jupyter-system-", "sungsoo-", "sungsu-")):
            continue
        
        bucket_objects = s3_client.list_objects(Bucket=bucket_name)
        bucket_size = 0
        last_accessed_date = "N/A"
        
        if 'Contents' in bucket_objects:
            objects_sizes = [objects['Size'] for objects in bucket_objects['Contents'] if objects.get("Size") is not None]
            if objects_sizes:
                for obj in objects_sizes:
                    bucket_size += obj
            bucket_size = round(bucket_size/(1000*1000), 2) # MB 단위
        
            last_accessed = [objects['LastModified'].strftime("%Y-%m-%d") for objects in bucket_objects['Contents'] if 'LastModified' in objects]
            if last_accessed:
                last_accessed_date = max(last_accessed)
                
        result = [bucket_name, bucket_size, last_accessed_date]
        bucket_result_list.append(result)
    orderer_bucket_result_list = sorted(bucket_result_list, key=lambda x: x[1], reverse=True)
    return orderer_bucket_result_list, bucket_name_max
                
                
def generate_mm_message(bucket_result_list, bucket_name_max):
    messages = []
    header = "S3 Bucket List - [" + str(len(bucket_result_list)) + " buckets]\n"
    crrent_time = datetime.now(timezone(timedelta(hours=9))).strftime('%Y-%m-%d %H:%M')
    header += (crrent_time+"\n")
    
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
    
    
def generate_curl_message(message):
    payload = {"text": message}
    return json.dumps(payload).encode("utf-8")
    
def generate_curl_message_block(message):
    payload = {"text": f'```{message}```'}
    return json.dumps(payload).encode("utf-8")

def post_message(url, data):
    req = urllib.request.Request(url)
    req.add_header("Content-Type", "application/json")
    return urllib.request.urlopen(req, data)


def lambda_handler(event, context):
    url = SLACK_URL
    
    bucket_result_list, bucket_name_max = get_s3_bucket()
    header, messages = generate_mm_message(bucket_result_list, bucket_name_max)
    
    data = generate_curl_message(header)
    response = post_message(url, data)
    
    for meg in messages:
        data = generate_curl_message_block(meg)
        response = post_message(url, data)
        
    return response.status
    #return message