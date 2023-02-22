import json
import adal
import requests
import boto3
import os
from base64 import b64decode
from datetime import datetime, timezone, timedelta

encrypted_clinentId = os.environ['clientId']
encrypted_tenantId = os.environ['tenantId']
encrypted_clientSecret = os.environ['clientSecret']
encrypted_subscriptionId = os.environ['subscriptionId']

decrypt_clientId = boto3.client('kms').decrypt(
    CiphertextBlob=b64decode(encrypted_clinentId),
    EncryptionContext={'LambdaFunctionName': os.environ['AWS_LAMBDA_FUNCTION_NAME']}
)['Plaintext'].decode('utf-8')

decrypt_tenantId = boto3.client('kms').decrypt(
    CiphertextBlob=b64decode(encrypted_tenantId),
    EncryptionContext={'LambdaFunctionName': os.environ['AWS_LAMBDA_FUNCTION_NAME']}
)['Plaintext'].decode('utf-8')

decrypt_clientSecret = boto3.client('kms').decrypt(
    CiphertextBlob=b64decode(encrypted_clientSecret),
    EncryptionContext={'LambdaFunctionName': os.environ['AWS_LAMBDA_FUNCTION_NAME']}
)['Plaintext'].decode('utf-8')

decrypt_subscriptionId = boto3.client('kms').decrypt(
    CiphertextBlob=b64decode(encrypted_subscriptionId),
    EncryptionContext={'LambdaFunctionName': os.environ['AWS_LAMBDA_FUNCTION_NAME']}
)['Plaintext'].decode('utf-8')


class AzureUsage:

    def __init__(self):
        scope = "subscriptions/" + decrypt_subscriptionId
        self.costmanagementUrl = "https://management.azure.com/" + scope + "/providers/Microsoft.CostManagement/query?api-version=2019-11-01"
        authority_uri = os.environ['activeDirectoryEndpointUrl'] + "/" + decrypt_tenantId
        context = adal.AuthenticationContext(authority_uri)
        token = context.acquire_token_with_client_credentials(
            os.environ["resourceManagerEndpointUrl"],
            decrypt_clientId,
            decrypt_clientSecret)
        bearer = "bearer " + token.get("accessToken")
        self.headers = {"Authorization": bearer, "Content-Type": "application/json"}
        self.usagedata = []

    def run(self, date, grain="Monthly"):

        payload = {
            "type": "ActualCost",
            "dataSet": {
                "granularity": grain,
                "aggregation": {
                    "totalCost": {
                        "name": "PreTaxCost",
                        "function": "Sum"
                    },
                    "totalCostUSD": {
                        "name": "PreTaxCostUSD",
                        "function": "Sum"
                    }
                }
            },
            "timeframe": "Custom",
            "timePeriod": {
                "from": date,
                "to": date
            }
        }

        payload['dataSet']['grouping'] = [{
            "type": "Dimension",
            "name": "ResourceGroupName"
        },
            {
                "type": "Dimension",
                "name": "ServiceName"
            },
            {
                "type": "Dimension",
                "name": "Meter"
            }
        ]

        payloadjson = json.dumps(payload)
        self.usagedata = []
        response = requests.post(self.costmanagementUrl, data=payloadjson, headers=self.headers)
        if response.status_code == 200:
            self.transform(payloadjson, response.text)
        else:
            print("error")
            print("error " + response.text)

        return self.usagedata

    def transform(self, payloadjson, response):
        result = json.loads(response)
        for record in result["properties"]["rows"]:
            usageRecord = {}
            for index, val in enumerate(record):
                columnName = result["properties"]["columns"][index]
                if columnName["type"] == "number":
                    usageRecord[columnName["name"]] = float(val)
                else:
                    usageRecord[columnName["name"]] = val

            self.usagedata.append(usageRecord)

        nextLink = result["properties"]["nextLink"]
        if nextLink != None:
            nextLinkResponse = requests.post(nextLink, data=payloadjson, headers=self.headers)
            if nextLinkResponse.status_code == 200:
                self.transform(payloadjson, nextLinkResponse.text)
            else:
                print("error in fetching next page " + nextLink)
                print("error " + nextLinkResponse.text)


def run_example():
    str_datetime = (datetime.now(timezone.utc).date() + timedelta(days=-1)).strftime('%Y/%m/%d')
    azure_usage = AzureUsage()
    usageResult = azure_usage.run(f"{str_datetime}", "daily")
    post_message(usageResult)


def post_message(usageResult):
    header = "Azure usage\nAcount: KMU@ddpslab.onmicrosoft.com\n"
    body = ""
    total = 0.0
    usageResult = sorted(usageResult, key=lambda d: d['PreTaxCostUSD'], reverse=True)
    for i in usageResult:
        body += f"{i['ResourceGroupName']} : {i['ServiceName']} : {i['Meter']} = {i['PreTaxCostUSD']}\n"
        total += float(i['PreTaxCostUSD'])
    total_header = f"Daily Total = {total}\n"
    send_to_slack(header + total_header + body)


def send_to_slack(msg):
    url = os.environ['web_hook']
    data = {'text': msg}
    resp = requests.post(url=url, json=data)


def lambda_handler(event, context):
    run_example()

