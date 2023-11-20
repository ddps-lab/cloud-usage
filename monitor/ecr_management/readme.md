# ecr management manual
이 문서는 aws의 Elastic Container Registry 자원을 관리하기 위한 설명서입니다.

## 1. 파일 소개
### aws_ecr_lambda_report_to_slack.py
- 실행하게 되는 메인 파일입니다.
- 사용자 지정 ECR 객체, 람다 객체를 받아와 조인후 슬랙에 전송합니다.
- 실행 중 오류가 생길 경우 에러 메세지를 슬랙에 전송합니다.
### get_ecr_object.py
- 사용자 지정 ECR 객체를 받아오는데 사용하는 파일입니다.
### get_lambda_object.py
- 사용자 지정 lambda 객체를 받아오는데 사용하는 파일입니다.
### slack_utils.py
- 슬랙으로 메시지를 전송하는 함수들이 있는 파일입니다.

## 2. 환경설정
1. aws lambda에서 실행할 시
	- `aws_ecr_lambda_report_to_slack.py`가 최종 lambda_function을 실행하는 파일입니다.
	- 핸들러 파일 이름을 위 파일으로 설정하거나, 위 파일의 이름을 `lambda_function.py`로 변경합니다.
	- 나머지 `get_ecr_object.py`, `get_lambda_object.py`, `slack_utils.py` 파일들도 함께 올려줍니다. 해당 파일들은 이름을 동일하게 해주어야 합니다.
	- Configuration -> Environment variable에 Key, Value 설정
		- key1 = EMAIL, value1 = 원하는 이메일
		- key2 = SLACK_DDPS, value2 = 보내고 싶은 slack hook url
	- ECR, Lambda, CloudWatch들에 대해 ReadOnly Access가 가능한 role을 할당해 줍니다.
2. 로컬에서 실행시
	- 필수적인 환경변수 설정
		- export EMAIL=원하는 이메일
		- export SLACK_DDPS=보내고 싶은 slack hook url
	- AWS 자격증명하기
		- 두 가지 방법
		1. 직접 환경변수 설정하기
			- export AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY
			- export AWS_SECRETE_ACCESS_KEY=YOUR_SECRETE_ACCESS_KEY
			- export AWS_DEFAULT_REGION=AWS_REGION_INFO
		2. credential file 설정
			- ~/.aws/credentials 파일에 다음 내용 넣기
			```
			[ddps-usage]
			aws_access_key_id = YOUR_ACCESS_KEY
			aws_secret_access_key = YOUR_SECRETE_ACCESS_KEY
			```
			- credential file을 설정하는 방식을 사용했다면 `aws_ecr_lambda_report_to_slack.py`의 `lambda_handler(event, context)`함수에서 첫 번째 라인의 `session = boto3.Session()`을 `session = boto3.Session(profile_name='ddps-usage')`로 변경해주어야 합니다.

## 3. 파일 설명
### 사용자 지정 객체
1. ECR 객체 형식
```
{
	REGION_NAME : {
		'repositories' : [
			{
				'repositoryName' : string,
				'images' : [
					{
						'imageTags' : [],
						'imageSizeGB' : int,
						'imagePushedAt' : datetime,
						'imageUris' : string
					},
				],
				'totalSizeGB' : int,
				'repositoryUri' : string,
				'lastPushedDate' : datetime
			},
		],
		'totalSizeGB' : int,
	}
} 
```
2. 람다 객체 형식
```
{
	REGION_NAME : [
		{
			'FunctionName' : string,
			'MemorySize' : int,
			'LastModified' : string,
			'PackageType' : string,
			'ImageUri' : string,
			'Description' : string,
			'LogGroupName' : string
		},
	]
}
```
	
### aws_ecr_lambda_report_to_slack.py
- `get_last_execution_time(client, log_group_name)`
	- 해당 로그 그룹의 마지막 로그 시간을 반환합니다. 반환하는 형식은 한국 UTC 시간입니다.
	- 함수 인자로 넣어주는 client는 CloudWatch의 boto3 client여야 합니다.
	- 로그 그룹이 없다면 "No Log Group"을 반환합니다.
	- 에러가 난다면 에러 메세지를 반환합니다.
- `get_repository_string(client, ecr_repository_object, lambda_region_object)`
	- ECR 리포지토리 객체와 람다 리전 객체를 조인하여 결과를 반환합니다. 반환 형식은 string입니다.
- `get_region_string(session, region, ecr_region_object, lambda_region_object)`
	- ECR 리전 객체와 람다 리전 객체를 조인하여 결과를 반환합니다. 반환 형식은 string입니다.
- `get_total_string(session, ecr_object, lambda_object)`
	- ECR 객체의 모든 리전을 순회하며 문자열화 한 후 최종 문자열을 반환합니다. 반환 형식은 string입니다.
- `lambda_handler(event, context)`
	- 내부 함수들을 실행 후 슬랙에 결과를 전송합니다.
### get_ecr_object.py
- `get_region_ecr_object(client, region)`
	- 리전에 맞는 ECR 리전 객체를 반환합니다. 반환 형식은 dictionary입니다.
	- 파라미터의 client는 ECR의 boto3 client여야 합니다.
	- 에러가 발생하면 (리전, 에러 메세지) 형식의 튜플을 반환합니다.
- `get_repository_object(client, repositoryName, repositoryUri)`
	- 리포지토리 이름에 맞는 ECR 리포지토리 객체를 반환합니다. 반환 형식은 dictionary 입니다.
	- 리포지토리 내부의 이미지들을 순회하며 이미지가 마지막으로 푸시된 시각을 리포지토리 객체에 저장합니다.
	- 에러가 발생하면 (리포지토리 이름, 에러 메세지) 형식의 튜플을 반환합니다.
- `get_region_ecr_object_dic(session)`
	- ECR 객체를 반환합니다. 반환 형식은 dictionary 입니다.
### get_lambda_object.py
- `get_region_lambda_object(client, region)`
	- 리전에 맞는 람다 리전 객체를 반환합니다. 반환 형식은 dictionary 입니다.
	- 에러가 발생하면 NoneType을 반환합니다.
- `get_region_lambda_object_dic(session)`
	- 람다 객체를 반환합니다. 반환 형식은 dictionary 입니다.
### slack_utils.py
- `send_message_to_slack(message)`
	- 파라미터로 들어온 메세지를 슬랙에 전송합니다.
	- 슬랙URL은 환경변수 `SLACK_DDPS`에 저장되어있어야 합니다.
	- 반환 형식은 HTTP response 형태입니다.
- `send_error_message_to_slack(message)`
	- 파라미터로 들어온 에러 메세지를 슬랙에 전송합니다.
	- 해당 함수는 석현님이 작성하신 코드를 살짝 변형하였습니다.
