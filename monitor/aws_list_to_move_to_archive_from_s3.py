import boto3
from botocore.exceptions import ClientError
import urllib.request, urllib.parse, json
from datetime import datetime, timezone, timedelta
import os

SLACK_URL = os.environ['SLACK_URL']
Target_Lambda = os.environ['Target_Lambda_ARN']

try:
    DEADLINE_MONTHS = int(os.environ['MONTHS'])
except KeyError:
    DEADLINE_MONTHS = 6
deadline = datetime.now() - timedelta(days=DEADLINE_MONTHS*30)


# s3 버킷을 불러오고 리스트로 목록을 만듭니다.
def get_final_archive_list_from_s3():
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


        if go_to_glacier == True:
            result = [bucket_name][0]
            if isinstance(result, list):
                result = result[0]
            bucket_result_list.append(result)    
            
    return bucket_result_list


# lambda_handler : lambda를 실행하고 다른 lambda로 목록을 보냅니다.
def lambda_handler(event, context):
    url = SLACK_URL
    
    bucket_result_list = get_final_archive_list_from_s3()
    target_lambda_arn = Target_Lambda
    
    lambda_client = boto3.client('lambda')
    response = lambda_client.invoke(
        FunctionName=target_lambda_arn,
        Payload=json.dumps(bucket_result_list)
    )
    return "The Archive List was sent in your Lambda Function. Check the your Lambda function event."