# This file watched your CloudTrail Log List.
import boto3
import json, urllib.request
from searchingInfo import callFunction
from globalValue import instancesLog, ENDTIME, STARTTIME, STARTDAY


# URL
SLACK_URL = ''


# checking cloudtrail one-day log
def oneDayLog(regions):
    
    # list up cloudtrail log of every region
    for region in regions:
        try:
            LogStartInstances(region)
            LogStopInstances(region)

        except KeyError as keyerror:
            print(f'{region} $ {keyerror}')
        except Exception as e:
            print(f"{region} > {e}")
    return 0


def LogStartInstances(region):
    cloudtrail = boto3.client('cloudtrail', region_name=region)
    response = cloudtrail.lookup_events(
        EndTime = ENDTIME,
        LookupAttributes = [
            {
                "AttributeKey": "EventName",
                "AttributeValue": "StartInstances"
            },
        ],
        StartTime = STARTTIME
    )

    for res in response['Events']:
        instanceId = res['Resources'][0]['ResourceName']
        if instanceId not in instancesLog:
            instancesLog[instanceId] = {'Username': res['Username'], 'state': [{'StartTime': res['EventTime']}]}
            callFunction(cloudtrail, instanceId)
        else:
            info = len(instancesLog[instanceId]['state']) - 1
            if res['EventTime'] != instancesLog[instanceId]['state'][info]['StartTime']:
                instancesLog[instanceId]['state'].append({'StartTime': res['EventTime']})

    return 0


def LogStopInstances(region):
    cloudtrail = boto3.client('cloudtrail', region_name=region)
    response = cloudtrail.lookup_events(
        EndTime = ENDTIME,
        LookupAttributes = [
            {
                "AttributeKey": "EventName",
                "AttributeValue": "StopInstances"
            },
        ],
        StartTime = STARTTIME
    )

    for res in response['Events']:
        instanceId = res['Resources'][0]['ResourceName']
        if instanceId in instancesLog:
            for info in range(0, len(instancesLog[instanceId]['state'])):
                if instancesLog[instanceId]['state'][info]['StartTime'] < res['EventTime']:
                    instancesLog[instanceId]['state'][info]['StopTime'] = res['EventTime']

    return 0



def creatingMessage():
    header = f"*Using Instances Log (DATE: {STARTDAY.strftime('%Y-%m-%d')})*"
    message = ""
    
    try:
        for instance in instancesLog:
            for info in range(0, len(instancesLog[instance]['state'])):
                ing = instancesLog[instance]['state'][info]['StopTime'] - instancesLog[instance]['state'][info]['StartTime']
                if ing.days == -1:
                    ing = (-ing)

                message += f"{instancesLog[instance]['awsRegion']} / {instancesLog[instance]['nameTag']} / {instancesLog[instance]['instanceType']} / {ing} 간 실행 / "

                if instancesLog[instance]['spotChecking'] == True:
                    message += "Spot :large_blue_diamond:\n"
                else:
                    message += "On-demand :large_orange_diamond:\n"

    except Exception as e:
        pass

    return header, message


def pushingSlack(message):
    payload = {"text": message}
    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(SLACK_URL)
    req.add_header("Content-Type", "application/json")
    return urllib.request.urlopen(req, data)


if __name__ == '__main__':
    # searching region
    ec2 = boto3.client('ec2')

    # creating region list
    regions = [ region['RegionName'] for region in ec2.describe_regions()['Regions']]
    oneDayLog(regions)

    #print(instancesLog)

    header, message = creatingMessage()
    pushingSlack(header)

    try:
        pushingSlack(message)
    except Exception as e:
        pushingSlack("Empty")