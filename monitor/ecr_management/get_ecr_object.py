import boto3
from datetime import datetime, timezone, timedelta

ecr_repository_errors = []
ecr_image_errors = []
korea_utc_timezone_info = timezone(timedelta(hours=9))

def get_region_ecr_object(client, region):
    print(f"get {region} region's ecr object")
    ret = { 'repositories': [], 'totalSizeGB': 0 }
    try:
        response = client.describe_repositories()
    except Exception as e:
        ecr_repository_errors.append((region, str(e)))
        return ret

    repositoryInfo = [(repo['repositoryName'], repo['repositoryUri']) for repo in response['repositories']]

    for repositoryName, repositoryUri in repositoryInfo:
        repository_object = get_repository_object(client, repositoryName, repositoryUri)
        ret['repositories'].append(repository_object)
        ret['totalSizeGB'] += repository_object['totalSizeGB']
    
    return ret

def get_repository_object(client, repositoryName, repositoryUri):
    ret = { 'repositoryName': repositoryName, 'images': [], 'totalSizeGB': 0, 'repositoryUri': repositoryUri }
    try:
        imageDetails = client.describe_images(repositoryName=repositoryName)['imageDetails']
    except Exception as e:
        ecr_image_errors.append((repositoryName, str(e)))
        return ret
    
    ret['lastPushedDate'] = datetime(1111, 1, 1, 1, 1, 1, tzinfo=korea_utc_timezone_info)
    
    for image in imageDetails:
        imageTags = image['imageTags'] if 'imageTags' in image.keys() else ['-']
        imageSizeGB = image['imageSizeInBytes'] / 1000000000.0
        imagePushedAt = image['imagePushedAt']
        ret['lastPushedDate'] = max(ret['lastPushedDate'], imagePushedAt)
        image_object = {'imageTags':imageTags, 'imageSizeGB':imageSizeGB, 'imagePushedAt':imagePushedAt, 'imageUris':None}
        uris = []
        for tag in imageTags:
            if tag != '-':
                uri = f"{repositoryUri}:{tag}"
            else:
                uri = f"{repositoryUri}@{image['imageDigest']}"
            uris.append(uri)
        image_object['imageUris'] = uris
        ret['images'].append(image_object)
        ret['totalSizeGB'] += imageSizeGB

    return ret


def get_region_ecr_object_dic(session):
    regions = session.get_available_regions('ecr')
    ret = {}
    for region in regions:
        client = session.client('ecr', region_name=region)
        region_object = get_region_ecr_object(client, region)
        if len(region_object['repositories']) <= 0:
            continue
        region_object['repositories'] = \
            sorted(region_object['repositories'], key=lambda x: (x['totalSizeGB'], x['lastPushedDate']), reverse=True)
        ret[region] = region_object
    return ret
    


if __name__ == "__main__":
    session = boto3.Session()
    region_ecr_object_dic = get_region_ecr_object_dic(session)
    for region in region_ecr_object_dic.keys():
        region_ecr_object = region_ecr_object_dic[region]
        repositories = region_ecr_object['repositories']
        for repository_object in repositories:
            images = repository_object['images']
            for image in images:
                print(image)
