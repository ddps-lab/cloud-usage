import json
from globalValue import instancesLog, STARTTIME


def callFunction(cloudtrail, runInstance):
    token, response = callLogsNotToken(cloudtrail, runInstance)

    result = searchingRunInstances(response, runInstance)

    if result:
        return 0
    else:
        while(result == False):
            token, response = callLogsToken(cloudtrail, runInstance, response['NextToken'])
            result = searchingRunInstances(response, runInstance)


def callLogsNotToken(cloudtrail, runInstance):
    response = cloudtrail.lookup_events(
        EndTime = STARTTIME,
        LookupAttributes = [
            {
                "AttributeKey": "ResourceName",
                "AttributeValue": runInstance
            },
        ]
    )

    token = False

    try:
        if response['NextToken']:
            token = True
    except Exception as e:
        pass

    return token, response


def callLogsToken(cloudtrail, runInstance, token):
    response = cloudtrail.lookup_events(
        EndTime = STARTTIME,
        LookupAttributes = [
            {
                "AttributeKey": "ResourceName",
                "AttributeValue": runInstance
            },
        ],
        NextToken = token
    )

    token = False

    try:
        if response['NextToken']:
            token = True
    except Exception as e:
        pass

    return token, response


def searchingRunInstances(response, runInstance):
    for event in response['Events']:
        if event['EventName'] == 'RunInstances':
            eventLog = json.loads(event['CloudTrailEvent'])
            instancesLog[runInstance]['awsRegion'] = eventLog['awsRegion']
            instancesLog[runInstance]['instanceType'] = eventLog['requestParameters']['instanceType']
            
            try:
                if eventLog['requestParameters']['instanceMarketOptions']['marketType']:
                    instancesLog[runInstance]['spotChecking'] = True
            except KeyError as keyerror:
                instancesLog[runInstance]['spotChecking'] = False

            try:
                nameTag = eventLog['requestParameters']['tagSpecificationSet']['items'][0]['tags'][0]['value']
                instancesLog[runInstance]['nameTag'] = nameTag
            except Exception as e:
                instancesLog[runInstance]['nameTag'] = instancesLog[runInstance]['Username']

            return True
    return False