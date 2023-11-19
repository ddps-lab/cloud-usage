import boto3
from datetime import datetime, timezone, timedelta
import json
import os
import urllib
import inspect

time_string_format = "%Y-%m-%d %H:%M"
six_month = timedelta(days=365/2)
one_year = timedelta(days=365)
korea_utc_timezone_info = timezone(timedelta(hours=9))
SLACK_URL = os.environ['SLACK_DDPS']
EMAIL = os.environ['EMAIL']

def send_message_to_slack(message):
    payload = {
        "text": message
    }
    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(SLACK_URL)
    req.add_header("Content-Type", "application/json")
    return urllib.request.urlopen(req, data)

def send_error_message_to_slack(message):
    module_name = inspect.stack()[1][1]
    line_no = inspect.stack()[1][2]
    function_name = inspect.stack()[1][3]

    msg = f"File \"{module_name}\", line {line_no}, in {function_name} :\n{message}"

    return send_message_to_slack(msg)
    
