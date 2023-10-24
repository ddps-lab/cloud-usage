# s3 management mennual
### 이 문서는 aws의 s3 버킷을 관리하기 위한 설명서입니다.


## 1. s3 management files
### aws_s3_standard_report.py
- 서울 리전의 람다 함수, `usage_s3_standard_report` 의 파일입니다.
- 서울 리전의 이벤트 브릿지인 `report_s3_bucket` 가 트리거로 존재합니다.
- 매월 1일 오전 10시마다 s3에 존재하는 standard class인 버킷의 정보를 슬랙으로 전송합니다.
- 버킷의 이름, standard class의 크기, 최근 액세스한 날을 확인할 수 있습니다.

### aws_s3_archiving_report.py
- 서울 리전의 람다 함수, `usage_s3_archiving_report` 의 파일입니다.
- 서울 리전의 이벤트 브릿지인 `report_s3_archiving` 가 트리거로 존재합니다.
- 짝수월 1일, 5일 오전 10시 5분마다 glacier class로 이동해야 하는 버킷의 정보를 슬랙으로 전송합니다.
- 버킷의 이름, standard class의 크기, 최근 액세스한 날을 확인할 수 있습니다.
- 기본 값으로 6개월이 지정되어 있어, 최근 6개월 간 액세스하지 않은 버킷이 glacier class로 이동할 버킷에 선정됩니다.

### aws_auto_s3_archiving.py
- 서울 리전의 스냅샷 안에 존재하는 `aws_auto_s3_archiving.py` 의 파일입니다.
- 짝수월 6일 오전 10시에 다음 작업을 실행하여 s3 버킷을 관리합니다.
- 스냅샷을 통해 AMI와 인스턴스를 생성한 후 s3 버킷을 자동으로 관리합니다.
- 최소 2분 정도의 시간이 소요됩니다.
- 사용이 끝났을 시 AMI와 인스턴스를 삭제해주십시오.


## 2. auto s3 archiving 방법
### 스냅샷을 이용하여 AMI 생성
1. 서울 리전 스냅샷에 존재하는 {Name : `usage_s3-management`, Description : `usage_s3-management(aws_auto_s3_archiving_snapshot)`} 스냅샷을 선택합니다.
2. `Actions` > `Create image from snapshot` 을 클릭합니다.
3. Image name 과 Description 을 작성합니다. (아래는 예시입니다.)
- Image name : s3-management
- Description : s3-management
4. 하단의 `Create image` 을 클릭하여 AMI를 생성합니다.

### AMI를 이용하여 인스턴스 생성 및 접근
1. AMIs에 존재하는 `AMI name : s3-management` 찾은 후 `Launch instance from AMI` 를 클릭합니다.
2. Name 과 Key pair, security group을 작성 또는 선택 후 `Lunch instance` 를 클릭하여 인스턴스를 생성합니다.
3. 이후 인스턴스를 편한 방법으로 접속하십시오.

### 인스턴스 내에서 s3 버킷 관리
1. 인스턴스 내에 존재하는 `aws_auto_s3_archiving.py` 내의 다음 내용을 찾아 수정합니다.
```
    # AWS 인증 정보 - 이곳에 사용자 정보를 반드시 입력하세요.
    aws_access_key_id = ''
    aws_secret_access_key = ''

    # 기간 설정 (ex : 6 개월) - 기간을 설정해주세요. (기본값 : 6)
    DEADLINE_MONTHS = 6

    # slack url - slack url을 입력해주세요. (기본값 : ddps labs)
    url = ''
```

- aws access key id : IAM 의 access key 입력
- aws secret access key : IAM 의 secret access key 입력
- 기간 및 슬랙의 URL은 필요시 변경하십시오.

- 비고) aws_access_key_id 과 aws_secret_access_key 찾기
    - aws 내의 IAM에서 Users를 클릭한 후 Security credentials의 Access keys 정보를 확인하십시오.

2. 다음 코드를 실행하여 s3 버킷을 관리합니다.
```
./environment.sh
python3 aws_auto_s3_archiving.py
```

3. 작업이 종료된 것을 확인 후 s3 버킷 관리를 종료합니다.


## 3. 사용한 자원 삭제
1. s3를 관리하기 위해 생성한 인스턴스를 `Terminated instance` 하십시오.
2. 인스턴스를 생성한 AMI를 `Actions` > `Deregister AMI` 하십시오.
3. (선택) Security Groups을 생성하였다면 정리하십시오.


# 주의 사항
1. AMI와 인스턴스를 삭제하지 않으면 과금의 요인이 될 수 있습니다.
2. `aws_auto_s3_archiving.py` 에 민감한 정보가 추가되오니 사용에 유의하십시오.
3. 자원을 삭제하여도 스냅샷이 유지된다면 언제든 다시 실행할 수 있습니다.
4. 스냅샷이 삭제되지 않도록 유의하십시오.