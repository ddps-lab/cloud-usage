# This file is reporting to you on daily instance usage used to cloud trail service.
# But cloud trail service is not frendly, so it's impossible to process duplicate searches.
# Therefore, please note that the code may be a little complicated and inefficient.

import boto3
import json, urllib.request, os
from datetime import datetime, timezone, timedelta
from aws_searched_instance_information import get_instance_information, get_run_instance_information
from slack_msg_sender import send_slack_message


# date information for searching daily logs in cloud trail service
TODAY = datetime.now(timezone.utc) + timedelta(hours=9)
ENDDAY = TODAY
STARTDAY = ENDDAY + timedelta(days=-1)

# transformated unix timestamp beacus of cloud trail service searching condition
ENDTIME = int(datetime(int(ENDDAY.strftime("%Y")), int(ENDDAY.strftime("%m")), int(ENDDAY.strftime("%d")), 15, 0, 0).timestamp())
STARTTIME = int(datetime(int(STARTDAY.strftime("%Y")), int(STARTDAY.strftime("%m")), int(STARTDAY.strftime("%d")), 15, 0, 0).timestamp())

SLACK_URL = os.environ['SLACK_DDPS']


# daily_instance_usage() : Collect instance information that 'run', 'start', 'terminate', and 'stop' for each region.
def daily_instance_usage(regions):
    all_daily_instance = {}
    searched_modes = ["RunInstances", "StartInstances", "TerminateInstances", "StopInstances"]
    
    # store cloud trail logs of all region
    for region in regions:
        try:
            for mode in searched_modes:
                all_daily_instance.update(search_instances(region, mode, all_daily_instance))

        except KeyError as keyerror:
            send_slack_message(f'daily_instance_usage() : KeyError in relation to {keyerror} in {region}')
        except Exception as e:
            send_slack_message(f'daily_instance_usage() : Exception in relation to {e} in {region}')
    return all_daily_instance


# search_instances() : Collect instance information and call the following functions.
def search_instances(region, mode, all_daily_instance):
    # search the logs on selected mode
    cloudtrail = boto3.client('cloudtrail', region_name=region)
    response = cloudtrail.lookup_events(
        EndTime = ENDTIME,
        LookupAttributes = [
            {
                "AttributeKey": "EventName",
                "AttributeValue": mode
            },
        ],
        StartTime = STARTTIME
    )

    # call the following functions according to the selected mode
    # parameter description : prevents duplicate searches, act the selected mode, and extracts data from results
    if mode == "RunInstances" or mode == "StartInstances":
        all_daily_instance = get_start_instances(mode, cloudtrail, response, all_daily_instance)
    else:
        all_daily_instance = get_stop_instances(cloudtrail, response, all_daily_instance)
    return all_daily_instance


# get_start_instances() : It stores the instance information of the 'creat' and 'start' state.
def get_start_instances(mode, cloudtrail, response, all_daily_instance):
    for events in response['Events']:

        # get instance id in result of cloud trail service
        if mode == "RunInstances":
            for resource in events['Resources']:
                if resource['ResourceType'] == 'AWS::EC2::Instance':
                    instance_id = resource['ResourceName']
        else:
            instance_id = events['Resources'][0]['ResourceName']
        
        # store new instance information
        event_time = events['EventTime'].replace(tzinfo=None)
        if instance_id not in all_daily_instance:
            all_daily_instance[instance_id] = {'UserName': events['Username'], 'state': [{'StartTime': event_time}]}
            if mode == "RunInstances":
                result, all_daily_instance = get_run_instance_information(response, instance_id, all_daily_instance)
            else:
                all_daily_instance = get_instance_information(cloudtrail, instance_id, all_daily_instance)

        # add the start time information of instance to daily instance list
        else:
            sequence = len(all_daily_instance[instance_id]['state']) - 1
            if event_time != all_daily_instance[instance_id]['state'][sequence]['StartTime']:
                all_daily_instance[instance_id]['state'].append({'StartTime': event_time})

    return all_daily_instance


# get_stop_instances() : It stores the instance information of the 'terminate' and 'stop' state.
def get_stop_instances(cloudtrail, response, all_daily_instance):
    for events in response['Events']:
        instance_id = events['Resources'][0]['ResourceName']
        event_time = events['EventTime'].replace(tzinfo=None)

        # add the stop time information of instance to daily instance list
        if instance_id in all_daily_instance:
            for info in range(0, len(all_daily_instance[instance_id]['state'])):
                if all_daily_instance[instance_id]['state'][info]['StartTime'] < event_time:
                    all_daily_instance[instance_id]['state'][info]['StopTime'] = event_time
        
        # store new instance information
        else:
            all_daily_instance[instance_id] = {'UserName': events['Username'], 'state': [{'StopTime': event_time}]}
            all_daily_instance = get_instance_information(cloudtrail, instance_id, all_daily_instance)
    return all_daily_instance


# create_message() : Create a message to send to Slack.
def create_message(all_daily_instance):
    header = f"*Daily Instances Usage Report (DATE: {STARTDAY.strftime('%Y-%m-%d')})*"
    message = ""
    
    try:
        for instance_id in all_daily_instance:
            for sequence in range(0, len(all_daily_instance[instance_id]['state'])):
                
                # when time information about start and stop be in all daily instance
                try:
                    run_time = all_daily_instance[instance_id]['state'][sequence]['StopTime'] - all_daily_instance[instance_id]['state'][sequence]['StartTime']

                # when time information about start or stop not be in all daily instance
                except KeyError:
                    try:
                        run_time = all_daily_instance[instance_id]['state'][sequence]['StopTime'] - datetime(int(STARTDAY.strftime("%Y")), int(STARTDAY.strftime("%m")), int(STARTDAY.strftime("%d")), 15, 0, 0)
                    except KeyError:
                        run_time = datetime(int(ENDDAY.strftime("%Y")), int(ENDDAY.strftime("%m")), int(ENDDAY.strftime("%d")), 15, 0, 0) - all_daily_instance[instance_id]['state'][sequence]['StartTime']

                if run_time.days == -1:
                    run_time = (-run_time)

                # create the message about instance usage
                message += f"{all_daily_instance[instance_id]['Region']} / {all_daily_instance[instance_id]['NameTag']} ({instance_id}) / {all_daily_instance[instance_id]['InstanceType']} / {run_time} 간 실행 / "

                # add emoji depending on whether spot instance is enabled
                if all_daily_instance[instance_id]['Spot'] == True:
                    message += "Spot :large_blue_diamond:\n"
                else:
                    message += "On-demand :large_orange_diamond:\n"

    except Exception as e:
        send_slack_message(f"created_message() : Exception in relation to {e}")

    return header, message


# push_slack() : Push a message to Slack.
def push_slack(message):
    payload = {"text": message}
    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(SLACK_URL)
    req.add_header("Content-Type", "application/json")
    return urllib.request.urlopen(req, data)


def lambda_handler(event, context):
    # searched region
    ec2 = boto3.client('ec2')

    # created region list and called main function
    regions = [ region['RegionName'] for region in ec2.describe_regions()['Regions']]
    all_daily_instance = daily_instance_usage(regions)

    # created message to slack and pushed to slack
    header, message = create_message(all_daily_instance)
    push_slack(header)

    # exception because of empty start instances or stop instances
    try:
        push_slack(message)
    except Exception:
        push_slack("Empty")

    return "perfect jobs. check the slack message, plz."