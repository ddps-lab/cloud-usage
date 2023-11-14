# ecr management manual
이 문서는 aws의 Elastic Container Registry 자원을 관리하기 위한 설명서입니다.

## 1. 파일 소개
### aws_ecr_report_to_slack.py
- 서울 리전의 람다 함수인 `usage_ecr_report`의 파일입니다.
- (이벤트 브릿지 추가할것)
- 각 region별 가지고있는 repository들의 목록을 크기 및 마지막으로 이미지가 푸쉬 된 시간을 기준으로 내림차순 정렬하여 슬랙으로 전송합니다.

## 2. 함수 설명
### 사용자 지정 객체
1. region 객체 형식
	- 'repositories' : list
	- 'totalSizeGB' : float
2. repository 객체 형식
	- 'repositoryName' : string
	- 'images' : list
	- 'totalSizeGB' : float
	- 'lastPushedDate' : datetime
3. image 객체 형식
	- 'imageTags' : list
	- 'imageSizeGB' : float
	- 'imagePushedAt' : datetime
	
### aws_ecr_report_to_slack.py
- `get_repository_object(client, repositoryName)`
	- 해당 이름의 repository 객체를 받아옵니다.
- `get_region_object(client, region)`
	- 해당 이름의 region 객체를 받아옵니다.
- `set_region_dict(session)`
	- 해당 계정에서 ecr repository가 존재하는 모든 region의 이름을 key로 객체를 생성하여 저장합니다.
- `get_region_string(name, region_object)`
	- name에 해당하는 region객체를 string 형태로 반환합니다.
- `get_repository_string(repository_object)`
	- 해당 repository 객체를 string 형태로 반환합니다.
- `get_image_string(image_object)`
	- 해당 image 객체를 string 형태로 반환합니다.
- `get_total_image_string()`
	- 모든 region 객체를 순회하며 repository, image객체까지 타고들어가 전부 string 형태로 만들어 합쳐서 반환합니다.
- `get_total_repository_string()`
	- 모든 region 객체를 순회하며 repository 객체까지 타고들어가 string 형태로 만들어 합쳐서 반환합니다.
- `send_message_to_slack(message)`
	- message를 슬랙으로 보냅니다. slack 주소는 환경변수로 설정되어있어야 합니다.
- `send_error_message_to_slack(message)`
	- error message를 stack trace형태로 슬랙으로 보냅니다. slack 주소는 환경변수로 설정되어있어야 합니다.
	
## 3. 환경설정
### aws_ecr_report_to_slack.py
1. aws lambda에서 실행할 시
	- 해당 코드를 code source에 넣습니다.
	- Configuration -> Environment variable에 Key, Value 설정
		- key1 = EMAIL, value1 = 원하는 이메일
		- key2 = SLACK_DDPS, value2 = 보내고 싶은 slack hook url
	- ECR Describe가 가능한 IAM role 할당
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
			- ```[ddps-usage]
aws_access_key_id = YOUR_ACCESS_KEY
aws_secret_access_key = YOUR_SECRETE_ACCESS_KEY```

## 4. 실행
### aws_ecr_report_to_slack.py
- 해당 파일은 aws lambda에서 실행됩니다.
- 실행 중 오류가 생길 경우 슬랙으로 에러 메세지를 전송합니다.
