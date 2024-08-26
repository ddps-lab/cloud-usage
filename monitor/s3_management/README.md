# s3 management mennual
### 이 문서는 aws의 s3 버킷을 관리하기 위한 설명서입니다.


## 1. s3 management files
### aws_s3_standard_report.py
- 서울 리전의 람다 함수, `usage_s3_standard_report` 의 파일입니다.
- 서울 리전의 이벤트 브릿지인 `usage_report_s3_bucket` 가 트리거로 존재합니다.
- 매월 1일 오전 10시마다 s3에 존재하는 standard class인 버킷의 정보를 슬랙으로 전송합니다.
- 버킷의 이름, standard class의 크기, 최근 액세스한 날을 확인할 수 있습니다.

### aws_s3_archiving_report.py
- 서울 리전의 람다 함수, `usage_s3_archiving_report` 의 파일입니다.
- 서울 리전의 이벤트 브릿지인 `usage_report_s3_archiving` 가 트리거로 존재합니다.
- 짝수월 1일, 5일 오전 10시 5분마다 glacier class로 이동해야 하는 버킷의 정보를 슬랙으로 전송합니다.
- 버킷의 이름, standard class의 크기, 최근 액세스한 날을 확인할 수 있습니다.
- 기본 값으로 6개월이 지정되어 있어, 최근 6개월 간 액세스하지 않은 버킷이 glacier class로 이동할 버킷에 선정됩니다.

### aws_auto_s3_archiving.py
- 서울 리전의 `usage_s3_management` ami 안에 존재하는 `aws_auto_s3_archiving.py` 의 파일입니다.
- 짝수월 6일 오전 10시에 ec2를 생성한 후 코드를 실행하여 s3 버킷을 관리합니다.
- 지정된 ami를 통해 `auto_archiving_management` 인스턴스를 생성한 후 s3 버킷을 자동으로 관리합니다.
- s3 버킷 관리 이후 ec2가 자동으로 삭제 됩니다.
- 서울 리전의 람다 함수, `usage_created_archiving_instance` 와 `usage_terminated_archiving_instance` 로 ec2를 관리합니다.
- 서울 리전의 이벤트 브릿지인 `usage_run_auto_archiving` 가 트리거로 존재합니다.
- IAM user `ddps-uasge` 의 정보를 이용하여 실행 자격을 증명합니다.
- 실행 시 최소 2분 정도의 시간이 소요될 수 있으며 파일 용량에 따라 실행 시간이 결정됩니다.

### usage_created_archiving_instance.py
- 서울 리전의 람다 함수, `usage_created_archiving_instance` 의 파일입니다.
- 트리거인 `usage_run_auto_archiving` 에 의해 실행되어 서울 리전에 S3를 관리할 수 있는 인스턴스를 생성합니다.
- t2.micro 타입의 one-time, terminate 설정의 스팟 인스턴스가 생성됩니다.

### usage_terminated_archiving_instance.py
- 서울 리전의 람다 함수, `usage_terminated_archiving_instance` 의 파일입니다.
- `aws_auto_s3_archiving.py` 내 작업이 끝나면 이 파일의 람다 함수를 호출하여 자동으로 실행됩니다.
- `usage_created_archiving_instance.py` 을 통해 생성된 인스턴스를 terminated 합니다.


## 2. Architecture
1. `report_s3_bucket` 트리거로 `usage_s3_standard_report` 람다 함수 실행
    매월 오전 10시에 standard class 버킷의 정보를 슬랙으로 전송
    
2. `report_s3_archiving` 트리거로 `usage_s3_archiving_report` 람다 함수 실행
    짝수월 1일과 5일 오전 10시 5분에 glacier class 로 이동해야 하는 버킷의 정보를 슬랙으로 전송

3-a. `run_auto_archiving` 트리거로 `usage_created_archiving_instance` 람다 함수 실행
    짝수월 6일 오전 10시에 ec2 를 생성하고 실행함

3-b. `auto_archiving_management` 이름의 ec2가 생성되고 2번의 결과인 버킷을 glacier class로 이동
    아카이빙 결과 및 5GB 가 넘어 수동으로 옮겨야 하는 파일에 대한 정보를 슬랙으로 전송

3-c. ec2 내에 존재하는 invoke lambda 를 통해 `usage_terminated_archiving_instance` 람다 함수 실행
    사용한 ec2를 종료시키고, 관리를 종료한다는 메세지를 슬랙으로 전송


## 3. 사용자 정의
모든 람다 함수 내에 각각 필요한 환경 변수가 지정되어 있습니다.
환경 변수는 사용자가 정할 수 있으며 수정 시 해당 변수를 사용하고 있는 모든 람다에 반영하여야 합니다.
아래는 현재 적용된 기본 값 혹은 예시입니다. 민감한 내용이나 수시로 변경될 가능성이 있는 정보는 비공개합니다.

```
AMI_ID = 'ami-00000'
DEADLINE_MONTHS = 6
PASS_LIST = ['bucket_name1', 'bucket_name2']
RUN_REGION = 'ap-northeast-2'
SLACK_DDPS = 'ddps-lab'
```

### 환경변수 이름 및 포함 함수
AMI_ID : usage_created_archiving_instance
DEADLINE_MONTHS : usage_s3_archiving_report, usage_created_archiving_instance
PASS_LIST : usage_s3_archiving_report, usage_created_archiving_instance
RUN_REGION : usage_created_archiving_instance, usage_terminated_archiving_instance
SLACK_DDPS : usage_s3_standard_report, usage_s3_archiving_report, usage_created_archiving_instance, usage_terminated_archiving_instance


# 주의 사항
1. 실행에 필요한 함수와 트리거가 수정 및 삭제되지 않도록 유의하십시오.
2. IAM user(`ddps-usage`), IAM roles(`usage-EBS-roles`, `usage-S3-roles`)이 삭제되지 않도록 유의하십시오.
3. `auto_archiving_management` 인스턴스 내에 민감한 정보가 담긴 파일이 있으니 유출하지 않도록 유의하십시오.
4. `usage_s3_management` ami와 연결된 스냅샷이 삭제되지 않도록 유의하십시오.