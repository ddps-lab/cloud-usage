from aws_ecr_lambda_report_to_slack import get_last_execution_time
import get_ecr_object
import get_lambda_object
import boto3
from datetime import datetime, timezone, timedelta

txt = "## 삭제 예정 ECR 리포지토리 및 lambda 함수\n"
txt += "- 삭제 조건 1 : 현재 이미지를 사용중인 람다 함수가 없고 마지막으로 이미지가 푸시 된 지 6개월이상 지난 경우\n"
txt += "- 삭제 조건 2 : 이미지를 사용하는 람다 함수의 마지막 실행 시각이 1년 이상 지난 경우\n"
six_month = 365/2
one_year = 365

delete_lambda = []
delete_ecr = []
alive_lambda = []
alive_ecr = []

if __name__ == "__main__":
    session = boto3.Session()
    ecr_object = get_ecr_object.get_region_ecr_object_dic(session)
    lambda_object = get_lambda_object.get_region_lambda_object_dic(session)

    repositoryNames_sorted_less = [(ecr_object[region]['totalSizeGB'], region) for region in ecr_object.keys()]
    repositoryNames_sorted_less.sort(reverse=True)

    now = datetime.now()

    for size, region in repositoryNames_sorted_less:
        txt += f"### {region}\n"
        client = session.client('logs', region_name=region)
        for repository in ecr_object[region]['repositories']:
            cur_use_lambda = []
            last_pushed_date = repository['lastPushedDate']
            repositoryName = repository['repositoryName']
            repositorySize = repository['totalSizeGB']
            for image in repository['images']:
                for imageUri in image['imageUris']:
                    for func in lambda_object.get(region):
                        if func['PackageType'] != 'Image':
                            continue
                        if func['ImageUri'] == imageUri:
                            cur_use_lambda.append(func)
            if len(cur_use_lambda) <= 0:
                if (now - timedelta(days=six_month)).astimezone(timezone(timedelta(hours=9))) > last_pushed_date:
                    txt += f"- ECR 리포지토리 : {repositoryName} / {repositorySize:.3f} GB\n"
                    delete_ecr.append(repositoryName)
                else:
                    alive_ecr.append(repositoryName)
            else:
                oldest_execution_time = datetime.min
                tmp_str = f"- ECR 리포지토리 : {repositoryName} / {repositorySize:.3f} GB\n"
                functions = []
                for func in cur_use_lambda:
                    lastexecution_time = get_last_execution_time(client, func['LogGroupName'])
                    if (type(lastexecution_time) == type(datetime.now())):
                        oldest_execution_time = max(oldest_execution_time, lastexecution_time)
                        lastexecution_time = lastexecution_time.strftime('%Y-%m-%d')
                    tmp_str += f"\t- 람다 함수 : {func['FunctionName']}, 마지막 실행시간 : {lastexecution_time}\n"
                    functions.append(func['FunctionName'])
                if (now - timedelta(days=one_year)).astimezone(timezone(timedelta(hours=9))) > last_pushed_date:
                    txt += tmp_str
                    delete_lambda = delete_lambda + functions
                    delete_ecr.append(repositoryName)
                else:
                    alive_lambda = alive_lambda + functions
                    alive_ecr.append(repositoryName)
    with open("tmp.md", "w") as f:
        f.write(txt)
    
    