# This file is reporting to you on daily instance usage used to cloud trail service.
# But cloud trail service is not frendly, so it's impossible to process duplicate searches.
# Therefore, please note that the code may be a little complicated and inefficient.

import boto3
import json, urllib.request, os
from datetime import datetime, timezone, timedelta
from slack_msg_sender import send_slack_message


SLACK_URL = os.environ['SLACK_DDPS']


# daily_instance_usage() : Collect instance information that 'run', 'start', 'terminate', and 'stop' for each region.
def daily_instance_usage(region, END_DATE):
    all_daily_instance = {}
    search_modes = ["RunInstances", "StartInstances", "TerminateInstances", "StopInstances"]
    
    # store cloud trail logs of all region
    try:
        for mode in search_modes:
            cloudtrail = boto3.client('cloudtrail', region_name=region)

            token, token_code = True, None
            while(token):

                if token == True and token_code == None:
                    token = False

                token, response = search_instances(cloudtrail, "EventName", mode, token, END_DATE + timedelta(days=-1), END_DATE, token_code)

                # call the following functions according to the selected mode
                # parameter description : prevents duplicate searches, act the selected mode, and extracts data from results
                if mode == "RunInstances" or mode == "StartInstances":
                    all_daily_instance = get_start_instances(mode, cloudtrail, response, all_daily_instance, END_DATE)
                else:
                    all_daily_instance = get_stop_instances(mode, cloudtrail, response, all_daily_instance, END_DATE)
                
                if response.get('NextToken') != None:
                    token_code = response['NextToken']
                    
    except KeyError as keyerror:
        send_slack_message(f'daily_instance_usage() : KeyError in relation to "{keyerror}" in "{region}"')
    except Exception as e:
        send_slack_message(f'daily_instance_usage() : Exception in relation to "{e}" in "{region}"')

    return all_daily_instance


# search_instances() : search the instance as cloud trail service.
def search_instances(cloudtrail, eventname, item, token, start_date, end_date, token_code):
    # transformated unix timestamp beacus of cloud trail service searching condition
    END_TIME = int(datetime(int(end_date.strftime("%Y")), int(end_date.strftime("%m")), int(end_date.strftime("%d")), 15, 0, 0).timestamp())
    START_TIME = int(datetime(int(start_date.strftime("%Y")), int(start_date.strftime("%m")), int(start_date.strftime("%d")), 15, 0, 0).timestamp())

    # search the instances
    response = []
    if token:
        response = cloudtrail.lookup_events(
            EndTime = END_TIME,
            LookupAttributes = [
                {
                    "AttributeKey": eventname,
                    "AttributeValue": item
                },
            ],
            StartTime = START_TIME,
            NextToken = token_code
        )
    else:
        response = cloudtrail.lookup_events(
            EndTime = END_TIME,
            LookupAttributes = [
                {
                    "AttributeKey": eventname,
                    "AttributeValue": item
                },
            ],
            StartTime = START_TIME
        )

    if response.get('NextToken') == None:
        token = False
    else:
        token = True
        
    return token, response


# get_start_instances() : It stores the instance information of the 'creat' and 'start' state.
def get_start_instances(mode, cloudtrail, response, all_daily_instance, END_DATE):
    for events in response['Events']:
        instance_ids, event_time = get_instance_ids(events)

        if instance_ids == None:
            continue

        for instance_id in instance_ids:
            # store new instance information
            if instance_id not in all_daily_instance:
                all_daily_instance[instance_id] = {'state': [{'StartTime': event_time}]}
                if mode == "RunInstances":
                    all_daily_instance = get_run_instance_information(events, instance_id, all_daily_instance)
                else:
                    all_daily_instance = search_instance_information(cloudtrail, instance_id, all_daily_instance, END_DATE)

            # add the start time information of instance to daily instance list
            else:
                # Ignore RunInstances event duplication
                if mode == "RunInstances":
                    continue
                sequence = len(all_daily_instance[instance_id]['state']) - 1
                if event_time != all_daily_instance[instance_id]['state'][sequence]['StartTime']:
                    all_daily_instance[instance_id]['state'].append({'StartTime': event_time})
    return all_daily_instance


# get_stop_instances() : It stores the instance information of the 'terminate' and 'stop' state.
def get_stop_instances(mode, cloudtrail, response, all_daily_instance, END_DATE):
    for events in response['Events']:

        instance_ids, event_time = get_instance_ids(events)

        if instance_ids == None:
            continue

        for instance_id in instance_ids:
            # add the stop time information of instance to daily instance list
            if instance_id in all_daily_instance:
                for sequence in range(0, len(all_daily_instance[instance_id]['state'])):
                    search_date = END_DATE + timedelta(days=-1)
                    search_start_time = datetime(int(search_date.strftime("%Y")), int(search_date.strftime("%m")), int(search_date.strftime("%d")), 15, 0, 0)
                    start_time = all_daily_instance[instance_id]['state'][sequence].get('StartTime')
                    if search_start_time == start_time and len(all_daily_instance[instance_id]['state']) == 1:
                        if all_daily_instance[instance_id]['state'][sequence].get('StopTime') > event_time:
                            del all_daily_instance[instance_id]
                            add_new_instance_information(cloudtrail, instance_id, all_daily_instance, event_time, END_DATE)
                        continue

                    if start_time < event_time:
                        all_daily_instance[instance_id]['state'][sequence]['StopTime'] = event_time
                    else:
                        previous_start_time = all_daily_instance[instance_id]['state'][sequence-1].get('StartTime')
                        previous_stop_time = all_daily_instance[instance_id]['state'][sequence-1].get('StopTime')
                        if previous_start_time != None and previous_start_time < event_time and previous_stop_time > event_time:
                            all_daily_instance[instance_id]['state'][sequence-1]['StopTime'] = event_time
            
            # store new instance information
            else:
                # Start only terminate
                if mode == "TerminateInstances":
                    continue
                add_new_instance_information(cloudtrail, instance_id, all_daily_instance, event_time, END_DATE)
    return all_daily_instance


# get_instance_ids() : Collect instance IDs to extract information for all instances in an event
def get_instance_ids(events):
    # get instance id in result of cloud trail service
    event_informations = json.loads(events['CloudTrailEvent'])
    instance_ids = []

    if event_informations.get('responseElements') == None:
        try:
            for resource in events['Resources']:
                if resource['ResourceType'] == 'AWS::EC2::Instance':
                    instance_ids.append(resource['ResourceName'])
        except KeyError:
            return None, 0
    
    else:
        instances = event_informations['responseElements']['instancesSet']['items']
        for n in range(len(instances)):
            instance_ids.append(instances[n]['instanceId'])
            
    event_time = events['EventTime'].replace(tzinfo=None)

    return instance_ids, event_time


# add_new_instance_information() : Collect information when the input instance has new information
def add_new_instance_information(cloudtrail, instance_id, all_daily_instance, event_time, END_DATE):
    search_date = END_DATE + timedelta(days=-1)
    search_start_time = datetime(int(search_date.strftime("%Y")), int(search_date.strftime("%m")), int(search_date.strftime("%d")), 15, 0, 0)
    all_daily_instance[instance_id] = {'state': [{'StartTime': search_start_time,'StopTime': event_time}]}
    all_daily_instance = search_instance_information(cloudtrail, instance_id, all_daily_instance, END_DATE)
    return all_daily_instance


# search_instance_information() : Call other functions to get information about the 'run instance'.
def search_instance_information(cloudtrail, run_instance_id, daily_instances, END_DATE):
    token, response = search_instances(cloudtrail, "ResourceName", run_instance_id, False, END_DATE + timedelta(days=-90), END_DATE, None)

    if token:
        while(token):
            token, response = search_instances(cloudtrail, "ResourceName", run_instance_id, token, END_DATE + timedelta(days=-90), END_DATE, response['NextToken'])
    for events in response['Events']:
        if events.get('EventName') == 'RunInstances':
            daily_instances = get_run_instance_information(events, run_instance_id, daily_instances)
    
    return daily_instances


# get_run_instance_information() : Store the necessary information from the extracted data.
def get_run_instance_information(events, run_instance_id, daily_instances):
    event_informations = json.loads(events.get('CloudTrailEvent'))
    daily_instances[run_instance_id]['InstanceType'] = event_informations['requestParameters'].get('instanceType')
    daily_instances[run_instance_id]['UserName'] = events.get('Username')

    if event_informations['requestParameters'].get('instanceMarketOptions') != None:
        daily_instances[run_instance_id]['Spot'] = True
    else:
        daily_instances[run_instance_id]['Spot'] = False

    try:
        name_tag = event_informations['requestParameters']['tagSpecificationSet']['items'][0]['tags'][0]['value']
        daily_instances[run_instance_id]['NameTag'] = name_tag
    except Exception:
        daily_instances[run_instance_id]['NameTag'] = daily_instances[run_instance_id]['UserName']

    return daily_instances


# create_message() : Create a message to send to Slack.
def create_message(all_daily_instance, search_date):
    message = ""
    count = 0
    try:
        for instance_id in all_daily_instance:
            for sequence in range(0, len(all_daily_instance[instance_id]['state'])):
                
                # when time information about start and stop be in all daily instance
                try:
                    run_time = all_daily_instance[instance_id]['state'][sequence]['StopTime'] - all_daily_instance[instance_id]['state'][sequence]['StartTime']

                # when time information about start or stop not be in all daily instance
                except KeyError:
                    if sequence == len(all_daily_instance[instance_id]['state']) - 1:
                        run_time = datetime(int(search_date.strftime("%Y")), int(search_date.strftime("%m")), int(search_date.strftime("%d")), 15, 0, 0) - all_daily_instance[instance_id]['state'][sequence]['StartTime']
                    else:
                        continue

                if run_time.days == -1:
                    run_time = (-run_time)

                # create the message about instance usage
                message += f"        {all_daily_instance[instance_id]['NameTag']} ({instance_id}) / {all_daily_instance[instance_id]['InstanceType']} / {run_time} 간 실행 / "

                # add emoji depending on whether spot instance is enabled
                if all_daily_instance[instance_id]['Spot'] == True:
                    message += "Spot :large_blue_diamond:\n"
                else:
                    message += "On-demand :large_orange_diamond:\n"
                count += 1

    except KeyError:
        send_slack_message("create_message() : A problem collecting instance information. Related functions is get_run_instance_information()")
    except Exception as e:
        send_slack_message(f"create_message() : Exception in relation to {e}")

    return message, count


# push_slack() : Push a message to Slack.
def push_slack(message):
    payload = {"text": message}
    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(SLACK_URL)
    req.add_header("Content-Type", "application/json")
    return urllib.request.urlopen(req, data)


def lambda_handler(event, context):
    # date information for searching daily logs in cloud trail service
    SEARCH_DATE = datetime.now(timezone.utc) + timedelta(days=-1, hours=9)
    header = f"*Daily Instances Usage Report (DATE: {SEARCH_DATE.strftime('%Y-%m-%d')})*"
    message = ""

    # created region list and called main function
    ec2 = boto3.client('ec2')
    regions = [ region['RegionName'] for region in ec2.describe_regions()['Regions']]
    for region in regions:
        all_daily_instance = daily_instance_usage(regions, SEARCH_DATE)

        # created message to slack and pushed to slack
        if len(all_daily_instance) != 0:
            usage_message,instance_count = create_message(all_daily_instance, SEARCH_DATE)
            message += f"{region} ({instance_count}\n)"
            message += usage_message

    push_slack(header)
    if message != "":
        push_slack(message)
    else:
        push_slack("Instances not used.")

    return "perfect jobs. check the slack message, plz."