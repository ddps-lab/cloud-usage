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


def get_final_archive_list_from_s3():
    s3_client = boto3.client('s3')
    all_bucket_list = s3_client.list_buckets()
    go_to_glacier_bucket_list = []
    
    for bucket in all_bucket_list['Buckets']:
        bucket_name = bucket["Name"]

        try:
            bucket_objects = s3_client.list_objects(Bucket=bucket_name)
        except (ClientError, NameError) as e:
            continue
        last_accessed_date = "N/A"
        go_to_glacier = True
        
        if 'Contents' in bucket_objects:
            last_accessed = [objects['LastModified'].strftime("%Y-%m-%d") for objects in bucket_objects['Contents'] if 'LastModified' in objects]
            if last_accessed:
                last_accessed_date = max(last_accessed)
                item_date = datetime.strptime(last_accessed_date, "%Y-%m-%d")
                if item_date > deadline:
                    go_to_glacier = False

        if go_to_glacier == True:
            result = [bucket_name][0]
            go_to_glacier_bucket_list.append(result)    
            
    return go_to_glacier_bucket_list

def go_to_glacier(go_to_glacier_bucket_list):
    s3_client = boto3.client('s3')

    all_bucket_list = s3_client.list_buckets()['Buckets']
    result_list = []
    bucket_name_max = 0

    for bucket in all_bucket_list:
        bucket_name = bucket["Name"]
        if bucket_name in go_to_glacier_bucket_list:
            bucket_name_max = max(bucket_name_max, len(bucket_name))
            bucket_size = 0

            objects = s3_client.list_objects_v2(Bucket=bucket_name)
            if 'Contents' in objects :   # 아무것도 없는 빈 버킷은 glacier 이동X
                for obj in objects.get('Contents', []):
                    object_key = obj['Key']
                    object_size = obj['Size']
                    object_storage_class = obj.get('StorageClass', 'STANDARD')
                    if object_size is not None:
                        bucket_size += object_size
                    if object_storage_class != 'GLACIER':
                        s3_client.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': object_key}, Key=object_key, StorageClass='GLACIER')

                bucket_size = round(bucket_size/(1000*1000), 2)  # MB 단위
                result = [bucket_name, bucket_size]
                result_list.append(result)
    ordered_result_list = sorted(result_list, key=lambda x: x[1], reverse=True)
    return ordered_result_list, bucket_name_max

def generate_message(go_to_glacier_bucket_list, bucket_name_max):
    messages = []
    len_list = len(go_to_glacier_bucket_list)
    header = "List of S3 Buckets successfully moved to Glacier - [" + str(len_list) + " buckets]\n"
    crrent_time = datetime.now(timezone(timedelta(hours=9))).strftime('%Y-%m-%d %H:%M')
    header += (crrent_time+"\n")
    
    if bucket_name_max < 11:
        bucket_name_max = 11
   
    if len_list == 0:
        message = "\nThere are NO Buckets moved to Glacier.\n"
    else:
        message = f'{"No":>2}. {"Bucket Name":{bucket_name_max+2}} {"Size":12}'
        count = 1
        for item in go_to_glacier_bucket_list:
            if item[1] >= 1000:
                item[1] = str(round(item[1]/1000, 2)) + " GB"
            else:
                item[1] = str(item[1]) + " MB"
            message += f'\n{count:>2}. {item[0]:{bucket_name_max+2}} {item[1]:12}'
            count += 1
            if len(message) > 3850:
                messages.append(message)
                message = ""
    messages.append(message)
    return header, messages
    

def generate_curl_message(message, meg_type):
    if meg_type == True:
        payload = {"text": message}
    else:
        payload = {"text": f'```{message}```'}
    return json.dumps(payload).encode("utf-8")


def post_message(url, data):
    req = urllib.request.Request(url)
    req.add_header("Content-Type", "application/json")
    return urllib.request.urlopen(req, data)


def lambda_handler(event, context):
    url = SLACK_URL

    go_to_glacier_bucket_list = get_final_archive_list_from_s3()
    result_list, bucket_name_max = go_to_glacier(go_to_glacier_bucket_list)
    header, messages = generate_message(result_list, bucket_name_max)
    
    data = generate_curl_message(header, True)
    response = post_message(url, data)
    for meg in messages:
        data = generate_curl_message(meg, False)
        response = post_message(url, data)
        
    return response.status
