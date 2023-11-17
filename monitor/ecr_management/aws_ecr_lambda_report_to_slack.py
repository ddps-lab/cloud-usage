import boto3
import get_lambda_object
from datetime import datetime, timedelta, timezone

def get_last_execution_time(client, log_group_name):
    try:
        # CloudWatch Logs 그룹에서 최근 로그 스트림 가져오기
        response_logs = client.describe_log_streams(
            logGroupName=log_group_name,
            orderBy='LastEventTime',
            descending=True
        )
    except Exception as e:
        return e
    
    dt = datetime.utcfromtimestamp(int(response_logs['logStreams'][0]['lastEventTimestamp'] / 1000.0))
    return dt
        
    
def test(client):
    response = client.describe_log_streams(
        logGroupName="/aws/lambda/cpucheck"
    )
    log_stream_name = response['logStreams'][0]['logStreamName']
    print(response)
    
    response_log = client.get_log_events(
        logGroupName="/aws/lambda/cpucheck",
        logStreamName=log_stream_name,
        #startFromHead=True,
        endTime=int(datetime.utcnow().timestamp())
    )
    print(response_log)
        
if __name__ == "__main__":
    session = boto3.Session()
    lambda_object_dic = get_lambda_object.get_region_lambda_object_dic(session)
    client = session.client('logs', region_name='us-east-2')
    us_east_2_lambda_object = lambda_object_dic['us-east-2']
    for lambda_object in us_east_2_lambda_object:
        last_execution_time = get_last_execution_time(client, lambda_object['LogGroupName'])
        print(f"{lambda_object['FunctionName']} last execution time : {last_execution_time}")
        
        
    