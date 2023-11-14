import boto3
from datetime import datetime, timezone, timedelta
import json
import os
import urllib
import inspect

region_dict_objects = {}

repository_errors = []
image_errors = []

# #region object
#   'repositories' : list
#   'totalSizeGB' : int

# repository_object
#   'repositoryName' : string
#   'images' : list
#   'totalSizeGB' : int
#   'lastPushedDate' : datetime

# image_object
#   'imageTags' : list
#   'imageSizeGB' : int
#   'imagePushedAt' : datetime

time_string_format = "%Y-%m-%d %H:%M"
six_month = timedelta(days=365/2)
one_year = timedelta(days=365)
korea_timezone = timezone(timedelta(hours=9))
SLACK_URL = os.environ['SLACK_DDPS']
EMAIL = os.environ['EMAIL']

def get_repository_object(client, repositoryName):
    korea_timezone = timezone(timedelta(hours=9))
    ret = { 'repositoryName': repositoryName, 'images': [], 'totalSizeGB': 0 }
    try:
        imageDetails = client.describe_images(repositoryName=repositoryName)['imageDetails']
    except Exception as e:
        image_errors.append((repositoryName, str(e)))
        return ret
    
    ret['lastPushedDate'] = datetime(1111, 1, 1, 1, 1, 1, tzinfo=korea_timezone)
    
    for image in imageDetails:
        imageTags = image['imageTags'] if 'imageTags' in image.keys() else ['-']
        imageSizeGB = image['imageSizeInBytes'] / 1000000000.0
        imagePushedAt = image['imagePushedAt']
        ret['lastPushedDate'] = max(ret['lastPushedDate'], imagePushedAt)
        image_object = {'imageTags':imageTags, 'imageSizeGB':imageSizeGB, 'imagePushedAt':imagePushedAt}
        ret['images'].append(image_object)
        ret['totalSizeGB'] += imageSizeGB

    return ret

def get_region_object(client, region):
    print(f"call get_region_object({region})")
    ret = { 'repositories': [], 'totalSizeGB': 0}
    try:
        response = client.describe_repositories()
    except Exception as e:
        repository_errors.append((region, str(e)))
        return ret

    names_repository = [repo['repositoryName'] for repo in response['repositories']]

    for repositoryName in names_repository:
        repository_object = get_repository_object(client, repositoryName)
        ret['repositories'].append(repository_object)
        ret['totalSizeGB'] += repository_object['totalSizeGB']
    
    return ret

def set_region_dict(session):
    regions = session.get_available_regions('ecr')
    for region in regions:
        client = session.client('ecr', region_name=region)
        region_object = get_region_object(client, region)
        if len(region_object['repositories']) <= 0:
            continue
        region_object['repositories'] = \
            sorted(region_object['repositories'], key=lambda x: (x['totalSizeGB'], x['lastPushedDate']), reverse=True)
        region_dict_objects[region] = region_object

def get_region_string(name, region_object):
    ret = f"\n====== REGION {name} total size : {region_object['totalSizeGB']:.3f} GB ======\n"
    return ret

def get_repository_string(repository_object):
    korea_date = repository_object['lastPushedDate'].astimezone(korea_timezone)
    formatted_date = korea_date.strftime(time_string_format)

    cur_korea_datetime = datetime.now(korea_timezone)
    deltatime = cur_korea_datetime - korea_date

    ret = f"repository name : {repository_object['repositoryName']} / "
    ret += f"repository size : {repository_object['totalSizeGB']:.3f} GB / "
    ret += f"last pushed date : {formatted_date}"

    if deltatime >= one_year:
        ret += " :red_circle:"
    elif deltatime >= six_month:
        ret += " :large_orange_circle:"
    else:
        ret += " :large_green_circle:"
    ret += "\n"

    return ret

def get_image_string(image_object):
    ret = f"\timage tags : {'/'.join(image_object['imageTags'])}, image size : {image_object['imageSizeGB']:.3f} GB, "
    korea_date = image_object['imagePushedAt'].astimezone(korea_timezone)
    formatted_date = korea_date.strftime(time_string_format)
    ret += f"imagePushedAt : {formatted_date}\n"
    return ret

def get_total_image_string():
    ret = ""
    for region in region_dict_objects.keys():
        region_object = region_dict_objects[region]
        ret += get_region_string(region, region_object)

        for repository_object in region_object['repositories']:
            ret += get_repository_string(repository_object)

            for image_object in repository_object['images']:
                ret += get_image_string(image_object)
    return ret

def get_total_repository_string():
    ret = ""
    for region in region_dict_objects.keys():
        region_object = region_dict_objects[region]
        ret += get_region_string(region, region_object)
        
        for repository_object in region_object['repositories']:
            ret += get_repository_string(repository_object)
    return ret

def send_message_to_slack(message):
    payload = {
        "text": message
    }
    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(SLACK_URL)
    req.add_header("Content-Type", "application/json")
    return urllib.request.urlopen(req, data)

def send_error_message_to_slack(message):
    module_name = inspect.stack()[1][1]
    line_no = inspect.stack()[1][2]
    function_name = inspect.stack()[1][3]

    msg = f"File \"{module_name}\", line {line_no}, in {function_name} :\n{message}"

    return send_message_to_slack(msg)

def lambda_handler(event, context):
    session = boto3.Session()

    cur_korea_datetime = datetime.now(korea_timezone)

    total_repository_string = "[ECR repository 사용 현황]\n"
    total_repository_string += f"Account: {EMAIL}\n"
    total_repository_string += cur_korea_datetime.strftime(time_string_format) + "\n"

    try:
        set_region_dict(session)
    except Exception as e:
        print("리전 객체를 초기화 하는데 실패했습니다")
        print(f"Error : {e}")
        send_error_message_to_slack(f"ECR region 객체 초기화 실패\n{e}")
        response = json.dumps({'message': e})
        return response
    
    try:
        total_repository_string += "마지막 이미지 푸시 경과 시간\n"
        total_repository_string += "6개월 미만 : :large_green_circle:\n"
        total_repository_string += "6개월 이상 : :large_orange_circle:\n"
        total_repository_string += "1년 이상 : :red_circle:"
        total_repository_string += get_total_repository_string()
        response = send_message_to_slack(total_repository_string)
        return response
    except Exception as e:
        print("Error at get_total_repository_string() or send_message_to_slack()")
        print(f"Error : {e}")
        send_error_message_to_slack(f"ECR repository 객체 문자열 변환 및 슬랙 전송 실패\n{e}")
        response = json.dumps({'message': e})
        return response


if __name__ == "__main__":
    response = lambda_handler(None, None)
    print(response)
    
