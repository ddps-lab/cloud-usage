import boto3
import get_lambda_object
import get_ecr_object
import os
import json
from datetime import datetime, timedelta, timezone
from slack_utils import send_message_to_slack, send_error_message_to_slack

time_string_format = "%Y-%m-%d %H:%M"
SLACK_URL = os.environ['SLACK_DDPS']
EMAIL = os.environ['EMAIL']

def get_last_execution_time(client, log_group_name):
    try:
        # CloudWatch Logs 그룹에서 최근 로그 스트림 가져오기
        response_logs = client.describe_log_streams(
            logGroupName=log_group_name,
            orderBy='LastEventTime',
            descending=True
        )
    except client.exceptions.ResourceNotFoundException as e:
        return "No Log Group"
    except Exception as e:
        return "error in get_last_execution_time() : " + str(e)
    
    dt_utc = datetime.utcfromtimestamp(int(response_logs['logStreams'][0]['lastEventTimestamp'] / 1000.0))
    dt_korea = dt_utc + timedelta(hours=9)
    return dt_korea
    
def get_repository_string(client, ecr_repository_object, lambda_region_object):
    ret = f"repository name : {ecr_repository_object['repositoryName']} / "
    ret += f"repository size : {ecr_repository_object['totalSizeGB']:.3f} GB / "
    ret += f"last pushed date : {ecr_repository_object['lastPushedDate']} / "
    cur_use_lambda = []
    
    if lambda_region_object != None:
        for image in ecr_repository_object['images']:
            for imageUri in image['imageUris']:
                for func in lambda_region_object:
                    if func['PackageType'] != 'Image':
                        continue
                    if func['ImageUri'] == imageUri:
                        cur_use_lambda.append(func)
                    
    if len(cur_use_lambda) <= 0:
        ret += f"current using lambda : None :red_circle:\n"
    else:
        ret += f"current using lambda : :large_green_circle:\n"
        for func in cur_use_lambda:
            name = func['FunctionName']
            last_execution_time = get_last_execution_time(client, func['LogGroupName'])
            if (type(last_execution_time) == type(datetime.now())):
                last_execution_time.strftime(time_string_format)
            ret += f"\t- function name : {name} / "
            ret += f"last execution time : {last_execution_time}\n"
            
    return ret

def get_region_string(session, region, ecr_region_object, lambda_region_object):
    try:
        client = session.client('logs', region_name=region)
    except Exception as e:
        print(f"get_region_string(). error : {e}")
        return None
    
    ret = f"\n====== REGION {region} Total Size : {ecr_region_object['totalSizeGB']:.3f} GB ======\n"
    for repository_object in ecr_region_object['repositories']:
        repository_string = get_repository_string(client, repository_object, lambda_region_object)
        ret += repository_string
    
    return ret

def get_total_string(session, ecr_object, lambda_object):
    ret = ""
    repositoryNames_sorted_less = [(ecr_object[region]['totalSizeGB'], region) for region in ecr_object.keys()]
    repositoryNames_sorted_less.sort(reverse=True)
    for size, region in repositoryNames_sorted_less:
        region_string = get_region_string(session, region, ecr_object[region], lambda_object.get(region))
        ret += region_string
    return ret

def lambda_handler(event, context):
    session = boto3.Session()

    try:
        ecr_object = get_ecr_object.get_region_ecr_object_dic(session)
    except Exception as e:
        print(e)
        send_error_message_to_slack(f"ECR 객체를 생성하는데 실패했습니다. error : {str(e)}")
        return json.dumps({'message' : str(e)})
    
    try:
        lambda_object = get_lambda_object.get_region_lambda_object_dic(session)
    except Exception as e:
        print(e)
        send_error_message_to_slack(f"람다 객체를 생성하는데 실패했습니다. error : {str(e)}")
        return json.dumps({'message' : str(e)})
    
    cur_korea_utc = datetime.now(timezone(timedelta(hours=9)))
    
    total_string = "[ECR repository 사용 현황]\n"
    total_string += f"Account: {EMAIL}\n"
    total_string += cur_korea_utc.strftime(time_string_format) + "\n"
    total_string += "해당 이미지와 연결된 람다 함수가 있는 경우 : :large_green_circle:\n"
    total_string += "해당 이미지와 연결된 람다 함수가 없는 경우 : :red_circle:\n"
    
    try:
        total_string += get_total_string(session, ecr_object, lambda_object)
    except Exception as e:
        print(e)
        send_error_message_to_slack(f"ECR, 람다 객체 문자열화에 실패했습니다. error : {str(e)}")
        return json.dumps({'message' : str(e)})
    
    try:
        response = send_message_to_slack(total_string)
        return response.status
    except Exception as e:
        print(e)
        send_error_message_to_slack(f"ECR, 람다 객체 문자열의 슬랙 전송에 실패했습니다. error : {str(e)}")
        return json.dumps({'message' : str(e)})
    
if __name__ == "__main__":
    response = lambda_handler(None, None)
    print(response)