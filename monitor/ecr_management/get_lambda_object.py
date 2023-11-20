import boto3
from datetime import datetime, timezone, timedelta

error_list = []

def get_region_lambda_object(client, region):
    ret = []
    functions = []
    marker = None
    try:
        while True:
            if marker:
                response = client.list_functions(Marker=marker)
            else:
                response = client.list_functions()
            functions.extend(response['Functions'])
            marker = response.get('NextMarker')
            if not marker:
                break
    except Exception as e:
        error_list.append((region, str(e)))
        return ret

    for function_object in functions:
        func = {
            'FunctionName': function_object['FunctionName'],
            'MemorySize': function_object.get('MemorySize'), 
            'LastModified': function_object['LastModified'], 
            'PackageType': function_object['PackageType'],
            'ImageUri': None,
            'Description': function_object.get('Description'),
            'LogGroupName': None
        }
        try:
            response_func = client.get_function(FunctionName=function_object['FunctionName'])
        except Exception as e:
            error_list.append((function_object['FunctionName'], str(e)))
            print(e)
            ret.append(func)
            continue
        
        if function_object['PackageType'] == 'Image':
            func['ImageUri'] = response_func['Code']['ImageUri']
        if 'LoggingConfig' in response_func['Configuration']:
            func['LogGroupName'] = response_func['Configuration']['LoggingConfig']['LogGroup']
        else:
            func['LogGroupName'] = f"/aws/lambda/{function_object['FunctionName']}"
        ret.append(func)
    return ret


def get_region_lambda_object_dic(session):
    regions = session.get_available_regions('lambda')
    ret = {}
    for region in regions:
        print(f"get {region} region's lambda_function_objects")
        client = session.client('lambda', region_name=region)
        region_object = get_region_lambda_object(client, region)
        if len(region_object) <= 0:
            continue
        ret[region] = region_object
    return ret


if __name__ == "__main__":
    session = boto3.Session()
    regions = session.get_available_regions('lambda')
    region_lambda_object_dic = get_region_lambda_object_dic(session)
    
    for region in region_lambda_object_dic.keys():
        print(f"====== REGION {region} ======")
        functions = region_lambda_object_dic[region]
        for function_object in functions:
            print(f"function name : {function_object['FunctionName']} / "\
                    f"PackageType : {function_object['PackageType']} / "\
                    f"ImageUri : {function_object['ImageUri']}")

    for error in error_list:
        print(error)
