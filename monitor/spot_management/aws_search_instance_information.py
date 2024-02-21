import json


# get_instance_information() : Call other functions to get information about the 'run instance'.
def get_instance_information(cloudtrail, run_instance_id, daily_instances):
    result = False
    token, response = get_run_instance(cloudtrail, run_instance_id)

    if token == False:
        result, daily_instances = get_run_instance_information(response, run_instance_id, daily_instances)
    else:
        while(token == True):
            token, response = get_next_run_instance(cloudtrail, run_instance_id, response['NextToken'])
        result, daily_instances = get_run_instance_information(response, run_instance_id, daily_instances)

    return daily_instances


# get_run_instance() : Get information on 'run instance' during the first search or 50 or fewer cloud trail service searches.
def get_run_instance(cloudtrail, run_instance_id):
    response = cloudtrail.lookup_events(
        LookupAttributes = [
            {
                "AttributeKey": "ResourceName",
                "AttributeValue": run_instance_id
            },
        ]
    )

    token = False

    try:
        if response['NextToken']:
            token = True
    except Exception:
        pass

    return token, response


# get_next_run_instance() : Get information from 'run instance' from more than 50 cloud trail service searches.
def get_next_run_instance(cloudtrail, run_instance_id, token_code):
    response = cloudtrail.lookup_events(
        LookupAttributes = [
            {
                "AttributeKey": "ResourceName",
                "AttributeValue": run_instance_id
            },
        ],
        NextToken = token_code
    )

    token = False

    try:
        if response['NextToken']:
            token = True
    except Exception:
        pass

    return token, response


# get_run_instance_information() : Store the necessary information from the extracted data.
def get_run_instance_information(response, run_instance_id, daily_instances):
    for events in response['Events']:
        if events['EventName'] == 'RunInstances':
            event_informations = json.loads(events['CloudTrailEvent'])
            daily_instances[run_instance_id]['Region'] = event_informations['awsRegion']
            daily_instances[run_instance_id]['InstanceType'] = event_informations['requestParameters']['instanceType']
            
            try:
                if event_informations['requestParameters']['instanceMarketOptions']['marketType']:
                    daily_instances[run_instance_id]['Spot'] = True
            except KeyError:
                daily_instances[run_instance_id]['Spot'] = False

            try:
                name_tag = event_informations['requestParameters']['tagSpecificationSet']['items'][0]['tags'][0]['value']
                daily_instances[run_instance_id]['NameTag'] = name_tag
            except Exception:
                daily_instances[run_instance_id]['NameTag'] = daily_instances[run_instance_id]['UserName']

            return True, daily_instances
    return False, daily_instances