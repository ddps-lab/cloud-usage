import boto3
import urllib.request, urllib.parse, json
from datetime import datetime, timedelta
import operator

query_start_day = 2
query_end_day = query_start_day - 1
account_name = "ddps@ddps.cloud"  # should be updated
slack_hook_url = ""  # should be updated

def get_usage():
    ce_client = boto3.client("ce")
    period = {
        "Start" : datetime.strftime(datetime.now() - timedelta(query_start_day), '%Y-%m-%d'),
        "End" : datetime.strftime(datetime.now() - timedelta(query_end_day), '%Y-%m-%d')
    }
    response = ce_client.get_cost_and_usage(TimePeriod=period, Granularity="DAILY",
                                            Metrics=["UnblendedCost"], GroupBy=[
                                                    {
                                                        'Type': 'DIMENSION',
                                                        'Key': 'SERVICE'
                                                    },
                                                    {
                                                        'Type': 'DIMENSION',
                                                        'Key': 'USAGE_TYPE'
                                                    }
                                                ])

    result = {}
    for r in response['ResultsByTime'][0]['Groups']:
        result.setdefault(r['Keys'][0], [])
        result[r['Keys'][0]].append({r['Keys'][1]: r['Metrics']['UnblendedCost']['Amount']})
    return result

def generate_slack_message(result):
    # 총 가격 계산
    total_price = sum(float(detail[list(detail.keys())[0]]) for service in result.values() for detail in service)

    # 서비스별 가격 계산 및 정렬
    service_prices = {service: sum(float(detail[list(detail.keys())[0]]) for detail in details)
                      for service, details in result.items()}
    sorted_services = sorted(service_prices.items(), key=lambda x: x[1], reverse=True)

    # 서비스별 세부 사항 정렬
    sorted_details = {service: sorted(details, key=lambda x: float(list(x.values())[0]), reverse=True)
                      for service, details in result.items()}

    # 결과 출력을 위한 문자열 생성
    output_str = "Acount: " + account_name + "\nDaily Total : " + str(total_price) + "$\n"

    for service, price in sorted_services:
        if price == 0:
            continue
        output_str += f"{service}: {price}$\n"
        for detail in sorted_details[service]:
            detail_name = list(detail.keys())[0]
            detail_price = float(list(detail.values())[0])
            if detail_price == 0:
                continue
            output_str += f"        {detail_name}: {detail_price}$\n"
        output_str += "\n\n"

    return output_str

def generate_curl_message(message):
    payload = {"text": message}
    return json.dumps(payload).encode("utf-8")

def post_message(url, data):
    req = urllib.request.Request(url)
    req.add_header("Content-Type", "application/json")
    return urllib.request.urlopen(req, data)

def lambda_handler(event, context):
    url = slack_hook_url
    report = get_usage()
    message = generate_slack_message(report)
    data = generate_curl_message(message)
    response = post_message(url, data)
    return response.status
    
