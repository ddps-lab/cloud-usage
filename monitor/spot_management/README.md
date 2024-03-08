# 기능
1. 현 계정의 모든 활성된 리전의 정보를 수집하고, 리스트로 만든다.
2. 활성화된 모든 리전을 리스트를 차례로 돌아 인스턴스 사용량을 수집한다.
3. 인스턴스의 정보를 이벤트 모드에 맞추어 수집하고, Dictionary로 저장한다.
4. Dictionary 정보를 토대로 인스턴스 사용량 메세지를 생성한다.
5. 활성화된 리전의 수만큼 3번과 4번을 반복한다.
6. 생성된 메세지를 토대로 슬랙에 전달한다.


## 설정해야 하는 관련 리소스
### Lambda (필수)
- 환경변수 : 메세지 전송 URL
- TimeOut : 15min *권장

### IAM Roles (필수)
- AmazonEC2ReadOnlyAccess
- AWSCloudTrail_ReadOnlyAccess
- AWSLambdaBasicExecutionRole

### event bridge (선택)
- 매일 아침 8시 40분에 동작하게 이벤트 스케줄 생성

### requests 모듈 (선택)
- slack_msg_sender 파일의 함수를 사용하기 위해 필요한 필수 모듈
- 사용 시 람다 레이어로 파이썬 버전에 맞게 설치 필요


# aws_daily_instance_usage_report.py
## values
- 사전에 람다 서비스 환경변수에 저장해둔 URL을 코드에서 사용하기 위해 선언한다.


## def daily_instance_usage(region, END_DATE):
    : Collect instance information that 'run', 'start', 'terminate', and 'stop' for each region.
- 인스턴스의 이벤트에는 'run, start, stop, terminate'의 4가지 종류가 존재한다.
- 각 리전에서 이 4가지 종류의 검색을 searched_instances() 함수를 통해 시도한다.
- 검색 횟수 : O(4n)

### 종류 별로 검색을 각각 시도하는 이유
- 클라우드 트레일 서비스는 안타깝게도 AND 검색이나 OR 검색이 불가능하다.
- 즉 Run Instance 이벤트이면서 i-00000000 인 인스턴스를 찾는 것은 불가능하다.
- 또한, Run Instance 이거나 Start Instance 인 인스턴스를 찾는 것 역시 불가능하다.
- 이러한 점 때문에, 각 리전에 4가지 종류를 시도한다.


## def search_instances(cloudtrail, eventname, item, token, start_date, end_date, token_code):
    : Collect instance information and call the following functions.
- 클라우드 트레일 서비스의 검색을 위해 시작 시간과 종료 시간을 UTC TIME 에서 UNIX TIMESTAMP 으로 변환한다.
- 설정된 이벤트 모드에 따라 실제 클라우드 트레일 서비스 API를 통해 검색을 시도한다.
- 검색 결과는 한 번에 50개의 응답만 확인할 수 있으며, 50개 이상부터는 토큰을 통해 추가 검색 후 확인할 수 있다.
- 추가 검색의 필요를 판단하기 위해 토큰 유무에 대한 결과와 응답을 반환한다.


## get_start_instances(mode, cloudtrail, response, all_daily_instance, END_DATE)
    : It stores the instance information of the 'creat' and 'start' state.
- 'run'과 'start' 이벤트로써 검색된 목록은 모두 검색일 당일 인스턴스가 켜졌다는 것을 의미한다.
- 인스턴스 사용량을 저장하기 위하여 모든 인스턴스 아이디를 Dictionary의 Key로 저장한다.
- 'run'과 'start' 이벤트의 응답 결과의 양식이 조금씩 다르기 때문에 이벤트 모드에 따라 인스턴스 아이디를 수집한다.

### 현재 이벤트의 인스턴스 아이디가 Key로 없는 경우
- 현재 이벤트가 인스턴스 사용량을 저장한 Dictionary에 없는 경우 사용량이 기록되지 않은 이벤트이기 때문에 이 이벤트에서 필요한 모든 정보를 추출한다.
- 이벤트 시작 시간을 일차적으로 저장한다.
- 이 외의 리전이나 인스턴스 유형(ex, t2.micro), 스팟 인스턴스 여부, Name 태그, Username은 이벤트 모드에 따라 함수를 호출하여 수집한다.
    - 리전이나 스팟 여부 등의 정보는 run instance event에서만 수집해올 수 있다.
    - 따라서 이미 검색한 이벤트 모드가 run instance 일 경우에는 get_run_instance_information() 함수를 호출하여 수집해온다.
    - start instance 일 경우에는 run instance를 검색하는 get_instance_information() 함수를 호출하여 현재 인스턴스 아이디와 일치하는 run instance 검색 결과에서 정보를 수집해온다.

### 현재 이벤트의 인스턴스 아이디가 Key로 저장되어 있는 경우
- 이미 리전이나 스팟 여부 등의 정보를 수집해온 인스턴스 아이디가 Dictionary에 존재하기 때문에 현재 이벤트 발생 시간만을 기록한다.
- 이미 인스턴스 아이디가 있는데, RunInstance 인 것은 이벤트 오류로 일반적이지 않은 경우이기에 무시한다.
- 지금 이벤트 시간이 인스턴스 정보 중 최근 Start Time 시간으로 저장되어 있지 않는다면 현재 이벤트 시간을 최신으로 반영하여 저장한다.


## def get_stop_instances(mode, cloudtrail, response, all_daily_instance, END_DATE):
    : It stores the instance information of the 'terminate' and 'stop' state.
- 'stop'과 'terminate' 이벤트로 검색된 목록은 모두 검색일 당일 인스턴스가 꺼졌다는 것을 의미한다.
- 인스턴스 사용량을 저장하기 위하여 모든 인스턴스 아이디를 Dictionary의 Key로 저장한다.
- 'stop'과 'terminate' 이벤트의 응답 결과는 완전히 일치하기 때문에 인스턴스 아이디가 Dictionary에 존재하는지만 판단한다.

### 현재 이벤트의 인스턴스 아이디가 Key로 저장되어 있는 경우
- 현재 이벤트가 Key로 저장되어 있다는 것은 검색하는 당일에 인스턴스가 켜졌다는 것을 의미한다.
- 검색 순서 상 'run' -> 'start' -> 'terminate' -> 'stop' 이기 때문에, 인스턴스 아이디가 존재한다는 것은 당일에 켜진 인스턴스가 존재한다는 의미이다.
- 따라서 현재 이벤트의 발생 시간을 Stop Time으로 저장하여 인스턴스의 종료 시간을 수집한다.
    - 현재 기록되어 있는 Start Time에 대응하는 Stop Time 값을 저장한다.
    - 특정한 조건을 통해 이벤트의 중복을 체크한다.
        1. 기록된 인스턴스의 시작 시간이 검색 시간과 동일하다.
        2. 인스턴스 사용량 기록이 단 한 개만 존재한다.
        - 이는 'stop' 이나 'terminate' 이벤트의 중복을 의미한다.
        - 기존에 존재하는 시간과 비교하여 더 빨리 실행된 이벤트의 시간을 Stop Time으로 기록한다.
    - Start Time 보다 Stop Time 이 더 늦게 저장되는지 확인하고 그 값을 기록한다.
    - Start Time 보다 빠른 Stop Time 값이 온다면, 이전에 저장한 이벤트의 중복이라는 의미임으로 이전에 저장한 Stop Time 값을 수정한다.

### 현재 이벤트의 인스턴스 아이디가 Key로 없는 경우
- 검색일 보다 이전에 시작된 인스턴스가 존재하여 검색 당일에는 시작된 인스턴스가 없는 경우 종료 이벤트임에도 불구하고 Dictionary에서 Key를 찾을 수 없다.
- 이 경우 사용량 리스트에 새로운 인스턴스의 정보를 추가하는 함수 add_new_instance_information()를 호출하여 인스턴스 정보를 기록한다.



## def get_instance_ids(events):
    : Collect instance IDs to extract information for all instances in an event
- 하나의 이벤트 값 안에 여러 개의 인스턴스가 실행된 경우, 모든 인스턴스의 아이디를 수집한다.
- 콘솔에서 2개 이상의 인스턴스에 run, start, stop, terminate를 할 시 하나의 이벤트로 기록되어 시간 수집이 누락되는 경우가 있었다.
- 이를 방지하고자 모든 인스턴스 아이디를 제대로 수집할 수 있도록 한다.


## def add_new_instance_information(cloudtrail, instance_id, all_daily_instance, event_time, END_DATE):
    : Collect information when the input instance has new information
- Run-Start-Terminate-Stop 순으로 이벤트를 수집하는데, Stop 또는 Terminate 이벤트를 실행할 때 검색 당일 Run 또는 Start 를 하지 않았을 경우 인스턴스의 정보 수집을 위해 실행한다.
- 코드 재사용성을 높이기 위해 함수로 제작하였다.
    - 같은 인스턴스에서 Terminate 후 중복으로 Terminate 되는 경우가 있다.
    - 리스트의 인덱스를 잘못 지정하였을 때 생기는 에러를 방지하고자 Run이나 Start를 하지 않았을 경우에는 인스턴스 목록 자체를 날린다.
    - 이때, 인스턴스 정보를 새로 생성하기 위한 코드를 실행해야 하기에, 재사용을 위해 함수로 제작하였다.


## def search_instance_information(cloudtrail, run_instance_id, daily_instances, END_DATE):
    : Call other functions to get information about the 'run instance'.
- 받아온 인스턴스 아이디를 토대로 인스턴스가 처음 생성되었을 때의 정보를 가지고 있는 run instance event를 찾는다.
- get_run_instance()를 호출하여 이벤트를 검색하는데, 인스턴스의 이벤트가 50개 이내일 경우 단 한 번의 함수 호출로도 이벤트를 찾아낼 수 있다.
- 50개 이내에 찾고자 했던 run instance event 정보가 있었다면 get_run_instance_information() 함수를 호출하여 인스턴스 정보를 수집한 후 이 정보를 반환한다.
- 만약 50개 이상의 이벤트가 존재하여 한 번의 검색으로 찾을 수 없었다면, 그 다음 50개를 검색하기 위해 존재하는 token 값을 이용하여 다음 50개 항목 중에 run instance event를 찾아낸다.
    - 이때, 인스턴스 아이디의 가장 첫번째 이벤트가 run instance event 라는 점을 이용하여 token 값이 없을 때까지 검색을 시도하고, token의 값이 없을 때의 검색 결과를 get_run_instance_information() 함수의 파라미터로 넣어 인스턴스의 정보를 알아낸다.


## def get_run_instance_information(events, run_instance_id, daily_instances):
    : Store the necessary information from the extracted data.
- 검색된 결과에서 인스턴스 아이디를 Key로 하여 Dictionary에 필요한 데이터를 저장한다.
- run instance event에서만 알 수 있는 정보가 저장되며, 인스턴스 유형(ex. t2.micro), 스팟 인스턴스 여부 및 네임 태그 정보를 수집할 수 있다.


## def get_spot_requests_information(region, instance_id, search_date):
    : Find the stop time recorded on spot request.
- 스팟 리퀘스트 요청 시 캔슬 시간을 지정한 경우 Terminate event 가 기록되지 않는다.
- 이를 찾기 위해 스팟 리퀘스트 요청에 포함된 캔슬 시간 정보를 검색해 온다.
- 모든 과정에서 캔슬 시간을 찾지 못한 경우 캔슬 시간이 정의되어 있지 않은 것으로 판단되며, 값을 찾지 않는다.


## def create_message(all_daily_instance, search_date):
    : Create a message to send to Slack.
- 슬랙에 보낼 메세지를 생성한다.
- message : 인스턴스 사용량을 저장한다.
- count : 인스턴스 사용 횟수를 저장한다.
- 인스턴스 사용량이 저장된 Dictionary를 순차적으로 돌며 메세지를 생성한다.

### 인스턴스 사용량 시간 메세지 생성
- 인스턴스 사용량은 일반적으로 시작부터 종료 시간까지를 기록한 후 종료 시간에서 시작 시간을 빼 계산한다.
- 그러나 일부 특이한 케이스는 다르게 계산한다.
    시작 시간은 존재하나 종료 시간이 없는 경우
    - 인스턴스의 Start Time은 기록되어 있으나 매치되는 Stop Time이 없을 경우에 해당한다.
    - 이 경우에는 인스턴스를 시작하였으나 종료를 하지 않았다는 것을 의미한다.
    - 이는 주로 마지막에만 실행될 수 있는 케이스이기 때문에, 저장된 시간이 마지막이 아니라면 Start Instance Event 중복으로 간주하고 무시한다.
    - 해당 사용량은 검색 기준으로 살펴보았을 때 인스턴스가 종료되지 않았다는 점을 참고하여 "인스턴스 실행 중" 이라고 확인된다.

### KeyError
- 검증되지 않은 케이스에서 KeyError가 발생할 수 있다. 이 경우 개발 과정에서 확인할 수 없었던 이벤트가 발생한 것임으로 인스턴스 정보를 수집하는 것과 관련된 함수를 찾아 디버그해야 한다.


## def push_slack(message):
    : Push a message to Slack.
- 슬랙에 메세지를 보낸다.


## def lambda_handler(event, context):
### 클라우드 트레일 서비스 검색 필터를 위한 DATE 상수 수집
- 클라우드 트레일 서비스에서 정확한 기간동안 로그를 수집하기 위해 UTC TIME을 기반으로 검색일을 수집한다.
- 검색 일자를 header 변수에 저장하여, 슬렉에 전달한다.

### 코드 동작 설명
- 현재 계정에 활성화된 모든 리전을 검색하고 각 리전을 확인한다.
- 인스턴스 사용량을 검색한다.
- 각 리전 별로 인스턴스 사용량을 받아오고, 슬랙에 만들 메세지로 생성한다.
- 실행한 인스턴스가 한 개 이상 존재하면 슬랙으로 메세지를 보낸다.
- 실행한 인스턴스가 한 개도 없을 시 인스턴스를 사용하지 않았다는 메세지를 보낸다.