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
    response=ce_client.get_cost_and_usage(TimePeriod=period, Granularity="DAILY", Metrics=["UnblendedCost"], GroupBy=[{"Type":"DIMENSION", "Key":"USAGE_TYPE"}])
    valid_r = response["ResultsByTime"][0]["Groups"]
    result = []
    for r in valid_r:
        if r["Metrics"]["UnblendedCost"]["Amount"] != "0":
            result.append(r)
    return result

def generate_mm_message(result):
    daily_total = 0.0
    temp_result = {}
    for r in result:
        temp_result[r["Keys"][0]] = float(r["Metrics"]["UnblendedCost"]["Amount"])
        daily_total += temp_result[r["Keys"][0]]
    sorted_result = dict(sorted(temp_result.items(), key=operator.itemgetter(1),reverse=True))

    message = "Acount: " + account_name + "\nDaily Total = " + str(daily_total) + "\n"
    for k in sorted_result.keys():
        message += (k + " = " + str(sorted_result[k]) + "\n")
    return message

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
    message = generate_mm_message(report)
    data = generate_curl_message(message)
    response = post_message(url, data)
    return response.status
    
