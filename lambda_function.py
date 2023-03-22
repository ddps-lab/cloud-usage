from google.oauth2 import service_account
from google.cloud import bigquery
from base64 import b64decode

import boto3
import requests
import json

kms = boto3.client('kms')

# 인증 정보 생성
credentials = service_account.Credentials.from_service_account_file(key_path)

def query():
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    saved_query = """
    SELECT
    project.id as PROJECT,
    location.region as REGION,
    service.description as SERVICE,
    CAST(export_time AS DATE) as DATE,
    sku.description as DESCRIPTION,
    SUM(cost) as COST
    FROM `tpu_billing_data.gcp_billing_export_v1_01C268_9BA8E9_8952A9`
    WHERE CAST(export_time AS DATE) =  DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    GROUP BY DATE, SERVICE, DESCRIPTION, REGION, PROJECT
    ORDER BY PROJECT, COST DESC, SERVICE
;
    """
    query_result = client.query(saved_query).to_dataframe()  # Make an API request.
    query_result['DATE'] = query_result['DATE'].astype(str)
    query_result = query_result.reset_index()
    total_bill = query_result['COST'].sum()
    query_result = query_result.drop(['DATE','index'], axis = 1).astype(str).to_dict()

    return query_result, total_bill

# Convert Query result dict to string
def converter(res_dict):
    res_str = ""
    for i in range(len(res_dict['PROJECT'])):
        for col, val in res_dict.items():
            if(col == 'COST'):
                res_str = res_str + val[i] + "\n"
            elif (col == 'SERVICE'):
                res_str = res_str + val[i]
            elif (col == 'DESCRIPTION'):
                res_str = res_str + "(" + val[i] + ") = "
            else:
                res_str = res_str + val[i] + " : "
    return res_str

url = 'WebHook URL'

#Slack Bot
def bot():
    usage, bill = query()
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

def lambda_handler():
    return bot()
