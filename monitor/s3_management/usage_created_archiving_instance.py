import boto3, os

def usage(user_data):
    region = os.environ['RUN_REGION']
    ami_id = os.environ['AMI_ID']

    ec2_client = boto3.client('ec2', region_name=region)

    response = ec2_client.run_instances(
        ImageId=ami_id,
        InstanceType='t2.micro',
        UserData=user_data,
        MinCount=1,
        MaxCount=1,
        InstanceMarketOptions={
            'MarketType':'spot',
            'SpotOptions':{
                'SpotInstanceType':'one-time',
                'InstanceInterruptionBehavior':'terminate'
            }
        }
    )

    instance_id = response['Instances'][0]['InstanceId']

    ec2_client.create_tags(
        Resources=[instance_id],
        Tags=[
            {'Key': 'Name', 'Value': 'auto_archiving_management'}
        ]
    )
    return instance_id


def created_userdata():
    DEADLINE_MONTHS = int(os.environ['DEADLINE_MONTHS'])
    SLACK_URL = os.environ['SLACK_DDPS']
    PASS_LIST = [item.strip() for item in os.environ['PASS_LIST'].split(',')]

    data = f"""#!/bin/bash
cat <<EOL > /home/ubuntu/config.ini
[s3_setting]
DEADLINE_MONTHS = {DEADLINE_MONTHS}
SLACK_URL = {SLACK_URL}
PASS_LIST = {PASS_LIST}
EOL
/home/ubuntu/runfile.sh
"""
    return data

    
def lambda_handler(event, context):
    data = created_userdata()
    instance_id = usage(data)
    
    return f"successfully create instance, id = {instance_id}"