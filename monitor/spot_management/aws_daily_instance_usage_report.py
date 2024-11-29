# This file is reporting to you on daily instance usage used to cloud trail service.
# But cloud trail service is not frendly, so it's impossible to process duplicate searches.
# Therefore, please note that the code may be a little complicated and inefficient.

import boto3
import json, urllib.request, os
from datetime import datetime, timezone, timedelta
from slack_msg_sender import send_slack_message


SLACK_URL = os.environ['SLACK_DDPS']


# daily_instance_usage() : Collect instance information that 'run', 'start', 'terminate', and 'stop' for each region.
def daily_instance_usage(region):
    all_daily_instance = {}
    search_modes = ["RunInstances", "StartInstances", "TerminateInstances", "StopInstances"]
    check_mode = [True, True, False, False]
    
    # store cloud trail logs of all region
    for i, mode in enumerate(search_modes):
        try:
            cloudtrail = boto3.client('cloudtrail', region_name=region)

            response_list = []
            token, response = search_instances(cloudtrail, "EventName", mode, False, 0, None)
            response_list.append(response)

            while(token):
                if response.get('NextToken') != None:
                    token_code = response['NextToken']

                token, response = search_instances(cloudtrail, "EventName", mode, token, 0, token_code)
                response_list.append(response)
        except:
            send_slack_message(f'An exception that occurred while getting the result of cloud trail query response about {mode} events in {region}')

        try:            
            # call the following functions according to the selected mode
            # parameter description : prevents duplicate searches, act the selected mode, and extracts data from results
            for response in response_list:
                if check_mode[i]:
                    all_daily_instance = get_start_instances(mode, cloudtrail, response, all_daily_instance)
                else:
                    all_daily_instance = get_stop_instances(mode, cloudtrail, response, all_daily_instance)

        except Exception as e:
            send_slack_message(f'An Exception that occurred while collecting instance usage information in {region}\n Check the error message: {e}')
    return all_daily_instance


# search_instances() : search the instance as cloud trail service.
def search_instances(cloudtrail, eventname, item, token, period, token_code):
    # search the instances
    response = []
    if token:
        response = cloudtrail.lookup_events(
            EndTime = end_datetime,
            LookupAttributes = [
                {
                    "AttributeKey": eventname,
                    "AttributeValue": item
                },
            ],
            StartTime = (start_datetime - timedelta(days=period)),
            NextToken = token_code
        )
    else:
        response = cloudtrail.lookup_events(
            EndTime = end_datetime,
            LookupAttributes = [
                {
                    "AttributeKey": eventname,
                    "AttributeValue": item
                },
            ],
            StartTime = (start_datetime - timedelta(days=period))
        )

    if response.get('NextToken') == None:
        token = False
    else:
        token = True
    return token, response


# get_start_instances() : It stores the instance information of the 'run' and 'start' state.
def get_start_instances(mode, cloudtrail, response, all_daily_instance):
    for events in response['Events']:
        instance_ids, event_time = get_instance_ids(events)

        try:
            if instance_ids == None:
                event_informations = json.loads(events['CloudTrailEvent'])
                if event_informations['responseElements'].get('omitted'):
                    request_number = (event_informations['requestParameters']['instancesSet'].get('items'))[0]['maxCount']
                    if 'SpotRquests' not in all_daily_instance:
                        all_daily_instance['SpotRquests'] = {'Number': request_number}
                    else:
                        all_daily_instance['SpotRquests']['Number'] += request_number
                continue
        except:
            send_slack_message(f'An exception that occurred in the process of determining how many spot request requests there were when there was no instance_id')

        try:
            for instance_id, spot_request_id in instance_ids:
                # store new instance information
                if instance_id not in all_daily_instance:
                    try:
                        all_daily_instance[instance_id] = {'state': [{'StartTime': event_time}], 'spot_request_id': spot_request_id}
                        if mode == "RunInstances":
                            all_daily_instance = get_run_instance_information(events, instance_id, all_daily_instance)
                        else:
                            all_daily_instance = search_instance_information(cloudtrail, instance_id, all_daily_instance)
                    except:
                        send_slack_message(f'An exception that occurred in storing the new instance information of {instance_id} during the "get_start_instances()" function.')

                # add the start time information of instance to daily instance list
                else:
                    # Ignore RunInstances event duplication
                    if mode == "RunInstances":
                        continue
                    try:
                        sequence = len(all_daily_instance[instance_id]['state']) - 1
                        if event_time != all_daily_instance[instance_id]['state'][sequence]['StartTime']:
                            all_daily_instance[instance_id]['state'].append({'StartTime': event_time})
                    except:
                        send_slack_message(f'An exception that occurred in storing the start instance event information of {instance_id}')
        except ValueError as e:
            send_slack_message(f'An exception that occurred in running "get_start_instances()" function.\nCheck the error message: {e}')

    return all_daily_instance


# get_stop_instances() : It stores the instance information of the 'terminate' and 'stop' state.
def get_stop_instances(mode, cloudtrail, response, all_daily_instance):
    for events in response['Events']:
        instance_ids, event_time = get_instance_ids(events)

        if instance_ids == None:
            continue
        try:
            for instance_id, _ in instance_ids:
                # add the stop time information of instance to daily instance list
                if instance_id in all_daily_instance:
                    try:
                        for sequence, instance_state in enumerate(all_daily_instance[instance_id]['state']):
                            start_time = instance_state.get('StartTime')
                            if start_datetime == start_time and len(all_daily_instance[instance_id]['state']) == 1:
                                if instance_state.get('StopTime') > event_time:
                                    del all_daily_instance[instance_id]
                                    add_new_instance_information(cloudtrail, instance_id, all_daily_instance, event_time)
                                continue

                            if start_time <= event_time:
                                instance_state['StopTime'] = event_time
                            else:
                                previous_start_time = all_daily_instance[instance_id]['state'][sequence-1].get('StartTime')
                                previous_stop_time = all_daily_instance[instance_id]['state'][sequence-1].get('StopTime')
                                if previous_start_time != None and previous_stop_time != None and previous_start_time < event_time and previous_stop_time > event_time:
                                    all_daily_instance[instance_id]['state'][sequence-1]['StopTime'] = event_time
                    except:
                        send_slack_message(f'An exception that occurred in getting the stop time information of {instance_id}.')

                # store new instance information
                else:
                    try:
                        # Start only terminate
                        if mode == "TerminateInstances":
                            continue
                        all_daily_instance = add_new_instance_information(cloudtrail, instance_id, all_daily_instance, event_time)
                    except:
                        send_slack_message(f'An exception that occurred in storing the new instance information of {instance_id} during the running "get_stop_instances()" function.')
        except ValueError as e:
            send_slack_message(f'An exception that occurred in running "get_stopt_instances()" function.\nCheck the error message: {e}')
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
                    instance_ids.append((resource['ResourceName'], None))
        except KeyError:
            return None, 0
    
    else:
        if event_informations['responseElements'].get('omitted'):
            return None, 0
        instances = event_informations['responseElements']['instancesSet']['items']
        for instance in instances:
            instance_ids.append((instance.get('instanceId'), instance.get('spotInstanceRequestId', '')))
            
    event_time = events['EventTime'].replace(tzinfo=timezone.utc)

    return instance_ids, event_time


# add_new_instance_information() : Collect information when the input instance has new information
def add_new_instance_information(cloudtrail, instance_id, all_daily_instance, event_time):
    all_daily_instance[instance_id] = {'state': [{'StartTime': start_datetime,'StopTime': event_time}]}
    all_daily_instance = search_instance_information(cloudtrail, instance_id, all_daily_instance)
    return all_daily_instance


# search_instance_information() : Call other functions to get information about the 'run instance'.
def search_instance_information(cloudtrail, run_instance_id, all_daily_instance):
    token, response = search_instances(cloudtrail, "ResourceName", run_instance_id, False, 88, None)

    if token:
        while(token):
            token, response = search_instances(cloudtrail, "ResourceName", run_instance_id, token, 88, response['NextToken'])
    for events in response['Events']:
        if events.get('EventName') == 'RunInstances':
            all_daily_instance = get_run_instance_information(events, run_instance_id, all_daily_instance)
    if all_daily_instance[run_instance_id].get('UserName') is None:
        for events in response['Events']:
            if events.get('EventName') in ['StartInstances', 'StopInstances', 'TerminateInstances']:
                all_daily_instance[run_instance_id]['UserName'] = events.get('Username')
                break
    
    return all_daily_instance


# get_run_instance_information() : Store the necessary information from the extracted data.
def get_run_instance_information(events, run_instance_id, all_daily_instance):
    event_informations = json.loads(events.get('CloudTrailEvent'))
    all_daily_instance[run_instance_id]['InstanceType'] = event_informations['requestParameters'].get('instanceType')
    all_daily_instance[run_instance_id]['UserName'] = events.get('Username')

    if event_informations['requestParameters'].get('instancesSet') != None:
        all_daily_instance[run_instance_id]['KeyName'] = event_informations['requestParameters']['instancesSet']['items'][0].get('keyName')

    if event_informations['requestParameters'].get('instanceMarketOptions') != None:
        all_daily_instance[run_instance_id]['Spot'] = True
    else:
        all_daily_instance[run_instance_id]['Spot'] = False

    try:
        name_tag = event_informations['requestParameters']['tagSpecificationSet']['items'][0]['tags'][0]['value']
        if name_tag[:4] == "sfr-":
            name_tag = "spot fleet"
        all_daily_instance[run_instance_id]['NameTag'] = name_tag
        if len(all_daily_instance[run_instance_id].get('UserName')) > 10:
            for resource in events.get('Resources'):
                if resource.get('ResourceType') == 'AWS::EC2::KeyPair':
                    all_daily_instance[run_instance_id]['UserName'] = resource.get('ResourceName')
    except Exception:
        all_daily_instance[run_instance_id]['NameTag'] = all_daily_instance[run_instance_id]['UserName']
        all_daily_instance[run_instance_id]['UserName'] = "aws"

    return all_daily_instance


# get_spot_requests_information() : Find the stop time recorded on spot request
def get_spot_requests_information(region, instance_id, request_id):
    try:
        cloudtrail = boto3.client('cloudtrail', region_name=region)
        if request_id == None:
            _, response = search_instances(cloudtrail, 'Username', instance_id, False, 0, None)
            for events in response['Events']:
                if events.get('EventName') == 'DescribeSpotInstanceRequests':
                    event_informations = json.loads(events.get('CloudTrailEvent'))
                    request_id = event_informations['requestParameters']['spotInstanceRequestIdSet']['items'][0].get('spotInstanceRequestId')
        _, response = search_instances(cloudtrail, 'ResourceName', request_id, False, 0, None)
        if response['Events'][0]['EventName'] == 'RequestSpotInstances':
            event_informations = json.loads(response['Events'][0].get('CloudTrailEvent'))
            valid_until = event_informations['requestParameters'].get('validUntil')
            stop_time = (datetime.fromtimestamp(valid_until/1000)).replace(microsecond=0, tzinfo=timezone.utc)
        return stop_time
    except:
        return None
    

# create_message() : Create a message to send to Slack.
def create_message(region, all_daily_instance):
    message = {'spot': ["",],  'request': "", 'on_demand': ["",]}
    instance_count = 0
    experiment_count = 0
    try:
        for instance_id in all_daily_instance:
            if instance_id == 'SpotRquests':
                message['request'] += f"{' ':>12}이외 스팟리퀘스트 요청이 {all_daily_instance['SpotRquests']['Number']}건 실행되었습니다.\n"
                continue
        
            for sequence, instance_state in enumerate(all_daily_instance[instance_id]['state']):
                state_running = False
                # when time information about start and stop be in all daily instance
                try:
                    run_time = instance_state['StopTime'] - instance_state['StartTime']

                # when time information about start or stop not be in all daily instance
                except KeyError:
                    if sequence == len(all_daily_instance[instance_id]['state']) - 1:
                        spot_request_id = all_daily_instance[instance_id].get('spot_request_id')
                        stop_time = get_spot_requests_information(region, instance_id, spot_request_id)
                        if stop_time != None:
                            run_time = stop_time - instance_state['StartTime']
                        else:
                            run_time = end_datetime - instance_state['StartTime']
                            if all_daily_instance[instance_id]['UserName'] == "InstanceLaunch" and all_daily_instance[instance_id]['KeyName'] == None:
                                run_time = timedelta(days=0, seconds=0)
                            else:
                                state_running = True
                    else:
                        continue

                if run_time.days == -1:
                    run_time = (-run_time)

                if run_time.seconds < 5:
                    instance_count += 1
                    experiment_count += 1
                    continue

                # create the message about instance usage
                if all_daily_instance[instance_id].get('InstanceType') is not None:
                    usage_message = f"{' ':>8}{all_daily_instance[instance_id]['NameTag']} ({all_daily_instance[instance_id]['UserName']}, {instance_id}) / {all_daily_instance[instance_id]['InstanceType']} / "
                else:
                    usage_message = f"{' ':>8}{all_daily_instance[instance_id]['UserName']} ({all_daily_instance[instance_id]['UserName']}, {instance_id}) / Not-Found / "

                if state_running:
                    usage_message += f"인스턴스 실행 중 ({run_time})\n"
                else:
                    usage_message += f"{run_time} 간 실행\n"

                # add message depending on whether spot instance is enabled
                if all_daily_instance[instance_id].get('Spot') in [True, None]:
                    if len(message['spot'][len(message['spot'])-1]) < 3950:
                        message['spot'][len(message['spot'])-1] += usage_message
                    else:
                        message['spot'].append(usage_message)
                else:
                    if len(message['on_demand'][len(message['on_demand'])-1]) < 3950:
                        message['on_demand'][len(message['on_demand'])-1] += usage_message
                    else:
                        message['on_demand'].append(usage_message)
                instance_count += 1

    except KeyError:
        send_slack_message(f"create_message() : A problem collecting instance information. Related functions is get_run_instance_information() in {region}")
    except Exception as e:
        send_slack_message(f"create_message() : Exception in relation to <{e}> in {region}")
    
    report_message = [f'{region} ({instance_count})\n']
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
    # setting datetime informations for searching daily logs in cloud trail service
    global search_datetime, start_datetime, end_datetime
    utc_datetime = datetime.now(timezone.utc)
    if utc_datetime.hour < 15:
        utc_datetime += timedelta(days=-1)
    search_datetime = utc_datetime + timedelta(hours=9)
    start_datetime = ((utc_datetime + timedelta(days=-1, hours=9)).astimezone(timezone(timedelta(hours=9)))).replace(hour=0, minute=0, second=0, microsecond=0)
    end_datetime = ((utc_datetime + timedelta(days=-1, hours=9)).astimezone(timezone(timedelta(hours=9)))).replace(hour=23, minute=59, second=59, microsecond=0)

    # creating head message
    header = f"*Daily Instances Usage Report (DATE: {search_datetime.strftime('%Y-%m-%d')})*"
    all_message = []
    stop_message = [False, "*생성된 인스턴스의 수가 많아 인스턴스 사용량 전달을 중단합니다.*"]

    # created region list and called main function
    ec2 = boto3.client('ec2')
    regions = [region['RegionName'] for region in ec2.describe_regions()['Regions']]
    for region in regions:
        if (datetime.now(timezone.utc) - utc_datetime).seconds > 270:
            stop_message[0] = True
            break
            
        all_daily_instance = daily_instance_usage(region)

        # created message to slack and pushed to slack
        if len(all_daily_instance) != 0:
            all_message.append(create_message(region, all_daily_instance))
         
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