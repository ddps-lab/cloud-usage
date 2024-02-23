# aws_daily_instance_usage_report.py
## values
### 1. SLACK 으로 메세지를 전송하기 위해 람다 서비스 환경변수에 저장된 URL 선언
- 사전에 람다 서비스 환경변수에 저장해둔 URL을 코드에서 사용하기 위해 저장한다.


## def daily_instance_usage(regions):
    : Collect instance information that 'run', 'start', 'terminate', and 'stop' for each region.
- 인스턴스의 이벤트에는 'run, start, stop, terminate'의 4가지 종류가 존재한다.
- 각 리전에서 이 4가지 종류의 검색을 searched_instances(region, mode, all_daily_instance) 함수를 통해 시도한다.
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
    - 따라서 이미 검색한 이벤트 모드가 run instance 일 경우에는 get_run_instance_information(response, instance_id, all_daily_instance) 함수를 호출하여 수집해온다.
    - 이 외에 start instance 일 경우에는 run instance를 검색하는 get_instance_information(cloudtrail, instance_id, all_daily_instance) 함수를 호출하여 현재 인스턴스 아이디와 일치하는 run instance 검색 결과에서 정보를 수집해온다.

### 현재 이벤트의 인스턴스 아이디가 Key로 저장되어 있는 경우
- 이미 리전이나 스팟 여부 등의 정보를 수집해온 인스턴스 아이디가 Dictionary에 존재하기 때문에 현재 이벤트 발생 시간 만을 기록한다.
- 지금 이벤트 시간이 인스턴스 정보 중 최근 Start Time 시간으로 저장되어 있지 않는다면 현재 이벤트 시간을 최신으로 반영하여 저장한다.


## def get_stop_instances(cloudtrail, response, all_daily_instance, END_DATE):
    : It stores the instance information of the 'terminate' and 'stop' state.
- 'stop'과 'terminate' 이벤트로 검색된 목록은 모두 검색일 당일 인스턴스가 꺼졌다는 것을 의미한다.
- 인스턴스 사용량을 저장하기 위하여 모든 인스턴스 아이디를 Dictionary의 Key로 저장한다.
- 'stop'과 'terminate' 이벤트의 응답 결과는 완전히 일치하기 때문에 인스턴스 아이디가 Dictionary에 존재하는지만 판단한다. (기능 수정 예정)

### 현재 이벤트의 인스턴스 아이디가 Key로 저장되어 있는 경우
- 현재 이벤트가 Key로 저장되어 있다는 것은 검색하는 당일에 인스턴스가 켜졌다는 것을 의미한다.
- 검색 순서 상 'run' -> 'start' -> 'terminate' -> 'stop' 이기 때문에, 인스턴스 아이디가 존재하기 위해서는 당일에 켜진 인스턴스가 존재해야 한다.
- 따라서 현재 이벤트의 발생 시간을 Stop Time으로 저장하여 인스턴스의 종료 시간을 수집한다.

### 현재 이벤트의 인스턴스 아이디가 Key로 없는 경우
- 검색일 보다 이전에 시작된 인스턴스가 존재하여 검색 당일에는 시작된 인스턴스가 없는 경우 종료 이벤트임에도 불구하고 Dictionary에서 Key를 찾을 수 없다.
- 이 경우 User 이름이나 이벤트 종료 시간을 저장한 후 리전이나 스팟 여부 등의 정보는 get_instance_information(cloudtrail, instance_id, all_daily_instance) 함수를 호출하여 수집해온다.


## def get_instance_ids(events):
    : Collect instance IDs to extract information for all instances in an event
- 하나의 이벤트 값 안에 여러 개의 인스턴스가 실행된 경우, 모든 인스턴스의 아이디를 수집한다.
- 콘솔에서 2개 이상의 인스턴스에 start, stop, terminate를 할 시 하나의 이벤트로 기록되어 시간 수집이 누락되는 경우가 있었다.
- 이를 방지하고자 모든 인스턴스 아이디를 제대로 수집할 수 있도록 한다.


## def add_new_instance_information(cloudtrail, instance_id, all_daily_instance, event_time, END_DATE):
    : Collect information when the input instance has new information
- Run-Start-Stop-Terminate 순으로 이벤트를 수집하는데, Stop 또는 Terminate 이벤트를 실행할 때 검색 당일 Run 또는 Start 를 하지 않았을 경우 인스턴스의 정보 수집을 위해 실행한다.
- 정확한 검색을 위해 확인하던 절차 중 코드 재사용성을 높이기 위해 함수로 제작하였다.
    - 같은 인스턴스가 간혹 중복으로 Terminate를 한 후 다시 Terminate를 하는 경우가 있다.
    - Run이나 Start를 하지 않았을 경우에는 인스턴스 목록 자체를 날리고 새로 생성하기 위해 함수 내 코드르 다시 반복할 필요가 있었고, 이를 위해 함수로 제작하여 재사용성을 높였다.


## def search_instance_information(cloudtrail, run_instance_id, daily_instances, END_DATE):
    : Call other functions to get information about the 'run instance'.
- 받아온 인스턴스 아이디를 토대로 인스턴스가 처음 생성되었을 run instance event를 찾는다.
- get_run_instance(cloudtrail, run_instance_id)를 호출하여 이벤트를 검색하는데, 인스턴스의 이벤트가 50개 이내일 경우 단 한 번의 함수 호출로도 이벤트를 찾아낼 수 있다.
- 50개 이내에 찾고자 했던 run instance event 정보가 있었다면 get_run_instance_information(response, run_instance_id, daily_instances) 함수를 호출하여 인스턴스 정보를 수집한 후 이 정보를 반환한다.
- 만약 50개 이상의 이벤트가 존재하여 한 번의 검색으로 찾을 수 없었다면, 그 다음 50개를 검색하기 위해 존재하는 token 값을 이용하여 다음 50개 항목 중에 run instance event를 찾아낸다.
    - 이때, 인스턴스 아이디의 가장 첫번째 이벤트가 run instance event 라는 점을 이용하여 token 값이 없을 때까지 검색을 시도하고, token의 값이 없을 때의 검색 결과를 get_run_instance_information(response, run_instance_id, daily_instances) 함수의 파라미터로 넣어 인스턴스의 정보를 알아낸다.


## def get_run_instance_information(response, run_instance_id, daily_instances):
    : Store the necessary information from the extracted data.
- 검색된 결과에서 인스턴스 아이디를 Key로 하여 Dictionary에 필요한 데이터를 저장한다.
- run instance event에서만 알 수 있는 정보가 저장되며, 리전이나 인스턴스 유형(ex. t2.micro), 스팟 인스턴스 여부 및 네임 태그 정보를 수집할 수 있다.


## def create_message(all_daily_instance):
    : Create a message to send to Slack.
- 슬랙에 보낼 메세지를 생성한다.
- header : 현재 이벤트의 이름과 검색일을 저장한다.
- message : 인스턴스 사용량을 저장한다.
- 인스턴스 사용량이 저장된 Dictionary를 순차적으로 돌며 메세지를 생성한다.

### 인스턴스 사용량 시간 메세지 생성
- 인스턴스 사용량은 일반적으로 시작부터 종료 시간까지를 기록한 후 종료 시간에서 시작 시간을 빼 계산한다.
- 그러나 일부 특이한 케이스는 다르게 계산한다.
1. 시작 시간은 존재하나 종료 시간이 없는 경우
    - 인스턴스의 Start Time은 기록되어 있으나 매치되는 Stop Time이 없을 경우에 해당한다.
    - 이 경우에는 인스턴스를 시작하였으나 종료를 하지 않았다는 것을 의미한다.
    - 해당 사용량은 인스턴스를 시작한 시간부터 검색이 종료되는 시간까지 측정하며, 이는 23시 59분 59초에서 시작한 시간을 뺀 값과 같다.
    - 예시 (시작 시간 : 10시 4분 49초, 종료 시간 : 없음)
        - 23:59:59 - 10:04:49 = 13:55:11
        - 총 17시간 13분 40초 동안 실행되었다.
2. 시작 시간이 없이 종료 시간만 존재하는 경우
    - 인스턴스의 Start Time이 기록되지 않은 채 Stop Time이 있을 경우에 해당한다.
    - 이 경우에는 인스턴스를 시작한 기록 없이 종료하였다는 것을 의미한다.
    - 주로 사용량 측정 이전에 켜두었다면 이런 일이 케이스가 존재할 수 있다.
    - 해당 사용량은 검색이 시작되는 시간부터 인스턴스가 종료된 시간까지 측정하며, 이는 종료 시간 자체와 같다.
    - 예시 (시작 시간 : 없음, 종료 시간 : 17시 13분 40초)
        - 17:13:40 - 00:00:00 = 17:13:40
        - 총 17시간 13분 40초 동안 실행되었다.


## def push_slack(message):
    : Push a message to Slack.
- 슬랙에 메세지를 보낸다.


## def lambda_handler(event, context):
### 1. 클라우드 트레일 서비스 검색 필터를 위한 DATE 상수 수집
- 클라우드 트레일 서비스에서 정확한 기간동안 로그를 수집하기 위해 UTC TIME을 기반으로 검색일을 수집한다.

### 코드 동작 설명
- 현재 계정에 활성화된 모든 리전을 검색하고 리스트로 만든다.
- 인스턴스 사용량을 검색한다.
- 슬랙에 만들 메세지를 생성한다.
- 슬랙으로 메세지를 보낸다.


# 기능
1. 현 계정의 모든 활성된 리전의 정보를 수집하고, 리스트로 만든다.
2. 활성화된 모든 리전을 리스트를 근거로 차례로 돌아 인스턴스 사용량을 수집한다.
3. 인스턴스의 정보를 이벤트 모드에 맞추어 수집하고, Dictionary로 저장한다.
4. Dictionary 정보를 토대로 인스턴스 사용량 메세지를 생성한다.
5. 생성된 메세지를 특정 슬랙으로 전달한다.