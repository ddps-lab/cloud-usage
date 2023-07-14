import boto3, re
import urllib.request, urllib.parse, json
from datetime import datetime, timezone, timedelta
import os
SLACK_URL = os.environ['SLACK_URL']

def get_running_instances():
    ec2_client = boto3.client('ec2')
    regions = [ region['RegionName'] for region in ec2_client.describe_regions()['Regions'] ]
    running_instances = []
    stopped_instances = []
    
    for region in regions:
        ec2_client = boto3.client('ec2', region_name=region)
        ec2_instances = ec2_client.describe_instances(Filters=[{'Name': 'instance-state-name', 'Values': ['running', 'stopped']}])
        for reservation in ec2_instances['Reservations']:
            for instance in reservation['Instances']:
                name_value = 'None'
                tags = instance.get('Tags', [])
                for tag in tags:
                    if tag['Key'] == 'Name':
                        name_value = tag['Value']
                        break
                instance_state = instance['State']['Name']
                current_time = datetime.now(timezone.utc)
                seoul_timezone = timezone(timedelta(hours=9))
                if instance_state == 'running':
                    launch_time = instance['LaunchTime'].replace(tzinfo=timezone.utc)
                    running_time = current_time - launch_time
                    days = running_time.days
                    hours = running_time.seconds // 3600
                    minutes = (running_time.seconds % 3600) // 60
                    instance_time = f"(Launch: {(launch_time+timedelta(hours=9)).strftime('%Y-%m-%d %H:%M')})" + f" ~ {days}일 {hours}시간 {minutes}분간 실행 중"
                    running_instances.append(region + " / " + name_value + " / " + instance['InstanceType'] + " / " + instance['InstanceId'] + " / " + instance_state + " / " + instance_time)
                else:
                    stopped_time = re.findall('.*\((.*)\)', instance['StateTransitionReason'])[0][:-4]
                    stopped_time = datetime.strptime(stopped_time, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                    running_time = current_time - stopped_time
                    days = running_time.days
                    hours = running_time.seconds // 3600
                    minutes = (running_time.seconds % 3600) // 60
                    instance_time = f"(Stopped: {(stopped_time+timedelta(hours=9)).strftime('%Y-%m-%d %H:%M')})" + f" ~ {days}일 {hours}시간 {minutes}분간 중지 중"
                    stopped_instances.append(region + " / " + name_value + " / " + instance['InstanceType'] + " / " + instance['InstanceId'] + " / " + instance_state + " / " + instance_time)
    
    return running_instances, stopped_instances
    

def generate_mm_message(running, stopped):
    message = "Account: bigdata@kookmin.ac.kr\n"
    current_time = datetime.now(timezone(timedelta(hours=9))).strftime('%Y-%m-%d %H:%M')
    message += (current_time+"\n")
    
    message += "\n[Running EC2 Instances]\n"
    for r in running:
        message += (r+" :large_green_circle:"+"\n")
    
    message += "\n[Stopped EC2 Instances]\n"
    for s in stopped:
        message += (s+" :white_circle:"+"\n")

    return message

def generate_curl_message(message):
    payload = {"text": message}
    return json.dumps(payload).encode("utf-8")

def post_message(url, data):
    req = urllib.request.Request(url)
    req.add_header("Content-Type", "application/json")
    return urllib.request.urlopen(req, data)

def lambda_handler(event, context):
    url = SLACK_URL
    
    running_instances, stopped_instances = get_running_instances()
    message = generate_mm_message(running_instances, stopped_instances)
    data = generate_curl_message(message)
    response = post_message(url, data)
    return response.status
    # return message
