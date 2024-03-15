# This file is reporting to you on daily instance usage used to cloud trail service.
# But cloud trail service is not frendly, so it's impossible to process duplicate searches.
# Therefore, please note that the code may be a little complicated and inefficient.

import boto3
import json, urllib.request, os
from datetime import datetime, timezone, timedelta
from slack_msg_sender import send_slack_message


SLACK_URL = os.environ['SLACK_DDPS']


# daily_instance_usage() : Collect instance information that 'run', 'start', 'terminate', and 'stop' for each region.
def daily_instance_usage(region, end_date):
    all_daily_instance = {}
    search_modes = ["RunInstances", "StartInstances", "TerminateInstances", "StopInstances"]
    
    # store cloud trail logs of all region
    try:
        for mode in search_modes:
            cloudtrail = boto3.client('cloudtrail', region_name=region)

            response_list = []
            token, response = search_instances(cloudtrail, "EventName", mode, False, end_date + timedelta(days=-1), end_date, None)
            response_list.append(response)

            while(token):
                if response.get('NextToken') != None:
                    token_code = response['NextToken']

                token, response = search_instances(cloudtrail, "EventName", mode, token, end_date + timedelta(days=-1), end_date, token_code)
                response_list.append(response)
            
            # call the following functions according to the selected mode
            # parameter description : prevents duplicate searches, act the selected mode, and extracts data from results
            for response in response_list:
                if mode == "RunInstances" or mode == "StartInstances":
                    all_daily_instance = get_start_instances(mode, cloudtrail, response, all_daily_instance, end_date)
                else:
                    all_daily_instance = get_stop_instances(mode, cloudtrail, response, all_daily_instance, end_date)
                
    except KeyError as keyerror:
        send_slack_message(f'daily_instance_usage() : KeyError in relation to "{keyerror}" in "{region}"')
    except Exception as e:
        send_slack_message(f'daily_instance_usage() : Exception in relation to "{e}" in "{region}"')

    return all_daily_instance


# search_instances() : search the instance as cloud trail service.
def search_instances(cloudtrail, eventname, item, token, start_date, end_date, token_code):
    # transformated unix timestamp because of cloud trail service searching condition
    end_time = int(datetime(int(end_date.strftime("%Y")), int(end_date.strftime("%m")), int(end_date.strftime("%d")), 15, 0, 0).timestamp())
    start_time = int(datetime(int(start_date.strftime("%Y")), int(start_date.strftime("%m")), int(start_date.strftime("%d")), 15, 0, 0).timestamp())

    # search the instances
    response = []
    if token:
        response = cloudtrail.lookup_events(
            EndTime = end_time,
            LookupAttributes = [
                {
                    "AttributeKey": eventname,
                    "AttributeValue": item
                },
            ],
            StartTime = start_time,
            NextToken = token_code
        )
    else:
        response = cloudtrail.lookup_events(
            EndTime = end_time,
            LookupAttributes = [
                {
                    "AttributeKey": eventname,
                    "AttributeValue": item
                },
            ],
            StartTime = start_time
        )

    if response.get('NextToken') == None:
        token = False
    else:
        token = True
    return token, response


# get_start_instances() : It stores the instance information of the 'run' and 'start' state.
def get_start_instances(mode, cloudtrail, response, all_daily_instance, END_DATE):
    for events in response['Events']:
        instance_ids, event_time = get_instance_ids(events)

        if instance_ids == None:
            event_informations = json.loads(events['CloudTrailEvent'])
            if event_informations['responseElements'].get('omitted'):
                request_number = (event_informations['requestParameters']['instancesSet'].get('items'))[0]['maxCount']
                if 'SpotRquests' not in all_daily_instance:
                    all_daily_instance['SpotRquests'] = {'Number': request_number}
                else:
                    all_daily_instance['SpotRquests']['Number'] += request_number
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
def get_stop_instances(mode, cloudtrail, response, all_daily_instance, end_date):
    for events in response['Events']:
        instance_ids, event_time = get_instance_ids(events)

        if instance_ids == None:
            continue

        for instance_id in instance_ids:
            # add the stop time information of instance to daily instance list
            if instance_id in all_daily_instance:
                for sequence in range(0, len(all_daily_instance[instance_id]['state'])):
                    search_date = end_date + timedelta(days=-1)
                    search_start_time = datetime(int(search_date.strftime("%Y")), int(search_date.strftime("%m")), int(search_date.strftime("%d")), 15, 0, 0)
                    start_time = all_daily_instance[instance_id]['state'][sequence].get('StartTime')
                    if search_start_time == start_time and len(all_daily_instance[instance_id]['state']) == 1:
                        if all_daily_instance[instance_id]['state'][sequence].get('StopTime') > event_time:
                            del all_daily_instance[instance_id]
                            add_new_instance_information(cloudtrail, instance_id, all_daily_instance, event_time, end_date)
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
                add_new_instance_information(cloudtrail, instance_id, all_daily_instance, event_time, end_date)
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
        if event_informations['responseElements'].get('omitted'):
            return None, 0
        instances = event_informations['responseElements']['instancesSet']['items']
        for n in range(len(instances)):
            instance_ids.append(instances[n]['instanceId'])
            
    event_time = events['EventTime'].replace(tzinfo=None)

    return instance_ids, event_time


# add_new_instance_information() : Collect information when the input instance has new information
def add_new_instance_information(cloudtrail, instance_id, all_daily_instance, event_time, end_date):
    search_date = end_date + timedelta(days=-1)
    search_start_time = datetime(int(search_date.strftime("%Y")), int(search_date.strftime("%m")), int(search_date.strftime("%d")), 15, 0, 0)
    all_daily_instance[instance_id] = {'state': [{'StartTime': search_start_time,'StopTime': event_time}]}
    all_daily_instance = search_instance_information(cloudtrail, instance_id, all_daily_instance, end_date)
    return all_daily_instance


# search_instance_information() : Call other functions to get information about the 'run instance'.
def search_instance_information(cloudtrail, run_instance_id, daily_instances, end_date):
    token, response = search_instances(cloudtrail, "ResourceName", run_instance_id, False, end_date + timedelta(days=-90), end_date, None)

    if token:
        while(token):
            token, response = search_instances(cloudtrail, "ResourceName", run_instance_id, token, end_date + timedelta(days=-90), end_date, response['NextToken'])
    for events in response['Events']:
        if events.get('EventName') == 'RunInstances':
            daily_instances = get_run_instance_information(events, run_instance_id, daily_instances)
    
    return daily_instances


# get_run_instance_information() : Store the necessary information from the extracted data.
def get_run_instance_information(events, run_instance_id, daily_instances):
    event_informations = json.loads(events.get('CloudTrailEvent'))
    daily_instances[run_instance_id]['InstanceType'] = event_informations['requestParameters'].get('instanceType')
    daily_instances[run_instance_id]['UserName'] = events.get('Username')

    if event_informations['requestParameters'].get('instancesSet') != None:
        daily_instances[run_instance_id]['KeyName'] = event_informations['requestParameters']['instancesSet']['items'][0].get('keyName')

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


# get_spot_requests_information() : Find the stop time recorded on spot request
def get_spot_requests_information(region, instance_id, search_date):
    try:
        cloudtrail = boto3.client('cloudtrail', region_name=region)
        token, response = search_instances(cloudtrail, 'Username', instance_id, False, search_date + timedelta(days=-1), search_date, None)
        for events in response['Events']:
            if events.get('EventName') == 'DescribeSpotInstanceRequests':
                event_informations = json.loads(events.get('CloudTrailEvent'))
                request_id = event_informations['requestParameters']['spotInstanceRequestIdSet']['items'][0].get('spotInstanceRequestId')
        token, response = search_instances(cloudtrail, 'ResourceName', request_id, False, search_date + timedelta(days=-1), search_date, None)
        if response['Events'][0]['EventName'] == 'RequestSpotInstances':
            event_informations = json.loads(response['Events'][0].get('CloudTrailEvent'))
            valid_until = event_informations['requestParameters'].get('validUntil')
            stop_time = (datetime.utcfromtimestamp(valid_until/1000)).replace(microsecond=0)
        return stop_time
    except:
        return None
    

# create_message() : Create a message to send to Slack.
def create_message(region, all_daily_instance, search_date):
    message = {'spot': ["",],  'request': "", 'on_demand': ["",]}
    instance_count = 0
    experiment_count = 0
    try:
        for instance_id in all_daily_instance:
            if instance_id == 'SpotRquests':
                message['request'] += f"{' ':>12}이외 스팟리퀘스트 요청이 {all_daily_instance['SpotRquests']['Number']}건 실행되었습니다.\n"
                continue
        
            for sequence in range(0, len(all_daily_instance[instance_id]['state'])):
                state_running = False
                
                # when time information about start and stop be in all daily instance
                try:
                    run_time = all_daily_instance[instance_id]['state'][sequence]['StopTime'] - all_daily_instance[instance_id]['state'][sequence]['StartTime']

                # when time information about start or stop not be in all daily instance
                except KeyError:
                    if sequence == len(all_daily_instance[instance_id]['state']) - 1:
                        stop_time = get_spot_requests_information(region, instance_id, search_date)
                        if stop_time != None:
                            run_time = stop_time - all_daily_instance[instance_id]['state'][sequence]['StartTime']
                        else:
                            run_time = datetime(int(search_date.strftime("%Y")), int(search_date.strftime("%m")), int(search_date.strftime("%d")), 15, 0, 0) - all_daily_instance[instance_id]['state'][sequence]['StartTime']
                            if all_daily_instance[instance_id]['UserName'] == "InstanceLaunch" and all_daily_instance[instance_id]['KeyName' == None]:
                                run_time = timedelta(days=0, seconds=0)
                            else:
                                state_running = True
                    else:
                        continue

                if run_time.days == -1:
                    run_time = (-run_time)

                if run_time.seconds < 3:
                    instance_count += 1
                    experiment_count += 1
                    continue

                # create the message about instance usage
                usage_message = f"{' ':>8}{all_daily_instance[instance_id]['NameTag']} ({instance_id}) / {all_daily_instance[instance_id]['InstanceType']} / "

                if state_running:
                    usage_message += f"인스턴스 실행 중 ({run_time})\n"
                else:
                    usage_message += f"{run_time} 간 실행\n"

                # add message depending on whether spot instance is enabled
                if all_daily_instance[instance_id]['Spot'] == True:
                    if len(message['spot'][len(message['spot'])-1]) < 3950:
                        message['spot'][len(message['spot'])-1] += usage_message
                    else:
                        message['spot'].append(usage_message)
                else:
                    if len(message['on_demand'][len(message['on_demand'])-1]) < 3950:
                        message['on_demand'][len(message['on_demand'])-1] += usage_message
                    else:
                        message['on_demand'].append(usage_message)
                count += 1

    except KeyError:
        send_slack_message("create_message() : A problem collecting instance information. Related functions is get_run_instance_information()")
    except Exception as e:
        send_slack_message(f"create_message() : Exception in relation to {e}")
    
    report_message = [f'{region} ({count})\n']
    for kind in message:
        if kind == 'request':
            report_message[len(report_message)-1] += message[kind]
            continue
        if kind == 'spot':
            emoji = ":large_blue_diamond:"
        else:
            emoji = ":large_orange_diamond:"
        for sequence in range(len(message[kind])):
            if message[kind][sequence] != "":
                if sequence == 0:
                    report_message[len(report_message)-1] += f"{' ':>8}{kind} {emoji}\n"
                message_block = f"```{message[kind][sequence]}```"
                if len(report_message[len(report_message)-1]) + len(message_block) < 4000:
                    report_message[len(report_message)-1] += message_block
                else:
                    report_message.append(message_block)
    if experiment_count > 0:
        report_message[len(report_message)-1] += f"{' ':4}실험을 위한 {experiment_count}개의 인스턴스가 3초 이내로 실행되었습니다.\n"

    return report_message
    

# push_slack() : Push a message to Slack.
def push_slack(message):
    payload = {"text": message}
    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(SLACK_URL)
    req.add_header("Content-Type", "application/json")
    return urllib.request.urlopen(req, data)


def lambda_handler(event, context):
    # date information for searching daily logs in cloud trail service
    record_time = datetime.now(timezone.utc) + timedelta(hours=9)
    search_date = datetime.now(timezone.utc) + timedelta(days=-1, hours=9)
    header = f"*Daily Instances Usage Report (DATE: {search_date.strftime('%Y-%m-%d')})*"
    all_message = []
    stop_message = [False, "*생성된 인스턴스의 수가 많아 인스턴스 사용량 전달을 중단합니다.*"]

    # created region list and called main function
    ec2 = boto3.client('ec2')
    regions = [region['RegionName'] for region in ec2.describe_regions()['Regions']]
    for region in regions:
        if ((datetime.now(timezone.utc) + timedelta(hours=9)) - record_time).seconds > 270:
            stop_message[0] = True
            break
        
        all_daily_instance = daily_instance_usage(region, search_date)

        # created message to slack and pushed to slack
        if len(all_daily_instance) != 0:
            all_message.append(create_message(region, all_daily_instance, search_date))
        
    push_slack(header)
    if len(all_message) != 0:
        for region_message in all_message:
            for message in region_message:
                push_slack(message)
        if stop_message[0]:
            push_slack(stop_message[1])
    else:
        push_slack("Instances not used.")

    return "perfect jobs. check the slack message, plz."