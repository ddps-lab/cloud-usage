import boto3
import json
import os
SLACK_URL = os.environ['SLACK_URL']

def get_running_instances():
    ec2_client = boto3.client('ec2')
    regions = [ region['RegionName'] for region in ec2_client.describe_regions()['Regions'] ]
    instances = []
    
    for region in regions:
        ec2_client = boto3.client('ec2', region_name=region)
        running_ec2 = ec2_client.describe_instances(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
        for reservation in running_ec2['Reservations']:
            for instance in reservation['Instances']:
                name_value = 'None'
                tags = instance.get('Tags', [])
                for tag in tags:
                    if tag['Key'] == 'Name':
                        name_value = tag['Value']
                        break
                instances.append(region + " / " + instance['InstanceId'] + " / " + instance['InstanceType'] + " / " + name_value)
    
    return instances
    

# def generate_mm_message(result):
#     daily_total = 0.0
#     temp_result = {}
#     for r in result:
#         temp_result[r["Keys"][0]] = float(r["Metrics"]["UnblendedCost"]["Amount"])
#     sorted_result = dict(sorted(temp_result.items(), key=operator.itemgetter(1),reverse=True))

#     message = "Acount: bigdata@kookmin.ac.kr\n"
#     for k in sorted_result.keys():
#         message += (k + " = " + str(sorted_result[k]) + "\n")

#     return message

# def generate_curl_message(message):
#     payload = {"text": message}
#     return json.dumps(payload).encode("utf-8")

# def post_message(url, data):
#     req = urllib.request.Request(url)
#     req.add_header("Content-Type", "application/json")
#     return urllib.request.urlopen(req, data)

def lambda_handler(event, context):
    url = SLACK_URL
    
    running_instances = get_running_instances()
    # message = generate_mm_message(running_instances)
    # data = generate_curl_message(message)
    # response = post_message(url, data)

    # return response.status
    return running_instances
