## boto3.Session사용을 위한 환경변수 설정
1. AWS_ACCESS_KEY_ID : AWS계정의 액세스 키
2. AWS_SECRETE_ACCESS_KEY : AWS계정의 비밀 키
3. AWS_SESSION_TOKEN : AWS 계정의 세션키. 임시 자격증명을 사용하는 경우 필요

### 해당하는 환경변수에 입력 해야함.
export AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
export AWS_DEFAULT_REGION=AWS_REGION_INFO

### 환경 변수 해제 방법
unset AWS_ACCESS_KEY_ID
unset AWS_SECRET_ACCESS_KEY
unset AWS_DEFAULT_REGION

## boto3가 자격증명을 확인하는 순서
---
1. boto3.client, resource, session 함수에 자격증명을 매개 변수로 직접 전달
2. 환경 변수
3. 공유 자격증명 파일 [~/.aws/credentials]
4. AWS 구성 파일 [~/.aws/config]
5. AssumeRole(임시 자격증명) 호출 [~/.aws/config]
6. Boto2 구성 파일 [/etc/boto.cfg 또는 ~/.boto]
7. IAM 역할이 구성된 Amazon EC2 인스턴스의 인스턴스 메타 데이터 서비스
