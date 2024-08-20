import boto3
import urllib.request, json, os


SLACK_DDPS = os.environ['SLACK_DDPS']

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

# lambda handler : 람다 실행
def lambda_handler(event, context):
    url = SLACK_DDPS
    region = os.environ['RUN_REGION']
    
    meg = ""
    instance_id = event['key']
    ec2_client = boto3.client('ec2', region_name=region)
    instances = ec2_client.terminate_instances(InstanceIds=[instance_id])
    
    for instance in instances['TerminatingInstances']:
        meg += "S3 관리를 안전하게 마무리합니다. 사용 리소스를 정리하였습니다."
        
    response = slack_message(meg, True, url)
    return "close the auto archiving management."