import boto3
from datetime import datetime, timezone, timedelta

region_dict_objects = {}

repository_errors = []
image_errors = []

# region object
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

def get_repository_object(client, repositoryName):
    ret = { 'repositoryName': repositoryName, 'images': [], 'totalSizeGB': 0, 'lastPushedDate': datetime(1999, 11, 24, 0, 0, 0) }
    try:
        imageDetails = client.describe_images(repositoryName=repositoryName)['imageDetails']
    except Exception as e:
        image_errors.append((repositoryName, str(e)))
        return ret
    
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
        region_dict_objects[region] = region_object

def get_region_string(name, region_object):
    ret = f"\n====== REGION {name} total size : {region_object['totalSizeGB']:.3f} GB ======\n"
    return ret

def get_repository_string(repository_object):
    korea_timezone = timezone(timedelta(hours=9))
    korea_date = repository_object['lastPushedDate'].astimezone(korea_timezone)
    formatted_date = korea_date.strftime("%Y-%m-%d %H:%M:%S")

    ret = f"repository name : {repository_object['repositoryName']} "
    ret += f"repository size : {repository_object['totalSizeGB']:.3f} GB "
    ret += f"last pushed date : {formatted_date}\n"
    
    return ret

def get_image_string(image_object):
    ret = f"\timage tags : {'/'.join(image_object['imageTags'])}, image size : {image_object['imageSizeGB']:.3f} GB, "
    korea_timezone = timezone(timedelta(hours=9))
    korea_date = image_object['imagePushedAt'].astimezone(korea_timezone)
    formatted_date = korea_date.strftime("%Y-%m-%d %H:%M:%S")
    ret += f"imagePushedAt : {formatted_date}\n"
    return ret

def get_total_string():
    ret = ""
    for region in region_dict_objects.keys():
        region_object = region_dict_objects[region]
        ret += get_region_string(region, region_object)

        for repository_object in region_object['repositories']:
            ret += get_repository_string(repository_object)

            for image_object in repository_object['images']:
                ret += get_image_string(image_object)

    return ret



if __name__ == "__main__":
    session = boto3.Session(profile_name='ddps-usage')
    set_region_dict(session)
    total_string = get_total_string()
    with open('ecr_repositories.txt', 'w') as f:
        f.write(total_string)
    print(total_string)
