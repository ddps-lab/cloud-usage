import requests
import json
import pandas
from google.cloud import bigquery
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

    FROM `bigquery_datatable`
    GROUP BY DATE, SERVICE, REGION, PROJECT
    ORDER BY PROJECT, DATE, SERVICE
    """
    query_job = client.query(saved_query).to_dataframe()  # Make an API request.
    query_job['DATE'] = query_job['DATE'].astype(str)

    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    yesterday_usage = query_job[query_job['DATE'] == yesterday].reset_index()
    yesterday_total_bill = yesterday_usage['COST'].sum()
    yesterday_usage = yesterday_usage.drop(['DATE','index'], axis = 1).astype(str).to_dict()

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

url = 'webhookurl'

def bot():
    usage, bill = test_query()
    usage = converter(usage)

    message = {
        'text': f"GCP Usage\nDaily Total: {bill}(KRW)\n{usage}"
    }

    response = requests.post(
        url=url,
        data=json.dumps(message),
        headers={'Content-Type': 'application/json'}
    )

    if response.status_code == 200:
        print('Slack message sent successfully')
    else:
        print('Error sending Slack message: {}'.format(response.text))
    return usage

def hello_world(request):
    request_json = request.get_json()
    return bot()
