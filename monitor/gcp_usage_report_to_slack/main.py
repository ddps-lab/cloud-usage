import slack_sdk
from slack_sdk.errors import SlackApiError
from google.cloud import bigquery
import pandas
from datetime import datetime, timezone, timedelta


def test_query():
    client = bigquery.Client()
    saved_query = """
    SELECT
    project.id as PROJECT,
    location.region as REGION,
    service.description as SERVICE,
    CAST(export_time AS DATE) as DATE,
    SUM(cost) as COST


    FROM `tpu_billing_data.gcp_billing_export_v1_01C268_9BA8E9_8952A9`
    GROUP BY DATE, SERVICE, REGION, PROJECT
    ORDER BY PROJECT, DATE, SERVICE
    """
    query_job = client.query(saved_query).to_dataframe()  # Make an API request.
    query_job['DATE'] = query_job['DATE'].astype(str)

    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    yesterday_usage = query_job[query_job['DATE'] == yesterday].reset_index()
    yesterday_total_bill = yesterday_usage['COST'].sum()
    yesterday_usage = yesterday_usage.drop(['DATE','index'], axis = 1)
    yesterday_usage = yesterday_usage.astype(str)
    yesterday_usage = yesterday_usage.to_dict()

    return yesterday_usage, yesterday_total_bill

def converter(res_dict):
    res_str = ""
    for i in range(len(res_dict['PROJECT'])):
        for col, val in res_dict.items():
            if(col == 'COST'):
                res_str = res_str + val[i] + "\n"
            elif (col == 'SERVICE'):
                res_str = res_str + val[i] + " = "
            else:
                res_str = res_str + val[i] + " : "
    return res_str

slack_token = "xoxb-4785135581590-4813638204354-JQhV3oQfBw0QUTmMNqE4PUPi"
channel_id = "C04PZCHQ9KJ"

def bot():
    # ID of the channel you want to send the message to
    bot_client = slack_sdk.WebClient(token = slack_token)
    try:
        # Call the chat.postMessage method using the WebClient
        usage, bill = test_query()
        usage = converter(usage)
        result = bot_client.chat_postMessage(
            channel=channel_id,
            text=f"GCP Usage\nDaily Total: {bill}(KRW)\n{usage}"
        )
    except SlackApiError as e:
        print(f"Error posting message: {e}")
    
    return "Done"

def hello_world(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    request_json = request.get_json()
    if request.args and 'message' in request.args:
        return request.args.get('message')
    elif request_json and 'message' in request_json:
        return request_json['message']
    else:
        return bot()
