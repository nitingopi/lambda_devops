# import pandas as pd

import requests
import json
import csv
import boto3
from botocore.exceptions import ClientError
import datetime



s3_client = boto3.client('s3')

BUCKET_NAME = 'conexus-reporting'
PREFIX = 'Conexus_Reports/'


def lambda_handler(event, context):
    CSV_FILE = '/tmp/'
    startDate = None
    endDate = None
    report_name = None
    campaign_list = []

    print(f"Printing event : {event}")

    # input_json = json.loads(event['body'])
    # print(f" Type of input_json {type(input_json)}")

    input_json = event
    if 'FROM_DATE' in input_json:
        print('FROM_DATE = ' + str(input_json['FROM_DATE']))
        startDate = str(input_json['FROM_DATE'])

    if 'TO_DATE' in input_json:
        print('TO_DATE = ' + str(input_json['TO_DATE']))
        endDate = str(input_json['TO_DATE'])

    if 'REPORT_NAME' in input_json:
        print('REPORT_NAME = ' + str(input_json['REPORT_NAME']))
        report_name = str(input_json['REPORT_NAME'])
        CSV_FILE = CSV_FILE + report_name + '.csv'
        report_name = PREFIX+report_name
        currentDT = datetime.datetime.now()
        report_name = f'{report_name}_' + currentDT.strftime("%Y%m%d%H%M%S")
        report_name += '.csv'
        print(f"CSV_FILE = {CSV_FILE}")

    if 'DIMENSIONS' in input_json:
        # no information how to use
        print(f'DIMENSIONS = {input_json["DIMENSIONS"]}')

    if 'METRICS' in input_json:
        # no information how to use
        print('METRICS = ' + str(input_json['METRICS']))

    if 'TOKEN' in input_json:
        print('TOKEN = ' + str(input_json['TOKEN']))
        auth_token = str(input_json['TOKEN'])

    if 'CAMPAIGN' in input_json:
        print('CAMPAIGN = ' + str(input_json['CAMPAIGN']))
        print(f"Type of CAMPAIGN {type(input_json['CAMPAIGN'])}")
        campaign_list = input_json['CAMPAIGN']

    URL1 = "https://mediahub.invidi.it/api/impressions/v1/timeseries/campaigns"
    URL2 = "https://mediahub.invidi.it/api/campaign-management/v1/campaigns"
    # auth_token_header_value = f'Bearer {auth_token}'
    auth_token_header = {"Authorization": auth_token}

    if startDate is not None and endDate is not None:
        print('startDate and endDate are not None')
        URL1 = f'{URL1}?startDate={startDate}&endDate={endDate}'

    if len(campaign_list) != 0:
        print('campaign_list is not Empty')
        for i in range(len(campaign_list)):
            URL1 = f'{URL1}&id={campaign_list[i]}' 

    # call API1 and get the list of campaign_id
    r1 = requests.get(url=URL1, headers=auth_token_header)

    # extracting data in json format
    data1 = r1.json()

    print(f"Printing data1 : {data1}")

    # return final_data
    final_response = []

    for item in data1:
        if "error" in item:
            return {
                "statusCode": 400,
                "headers": {
                    "Content-Type": "application/json"
                },
                "body": item["error"]["message"]
            }
        elif "metrics" in item:
            if item["id"] and item["id"] != None:
                pay_load = {"id": item["id"]}
                # call API2 corresponding to the campaign_id
                res = requests.get(URL2, params=pay_load,
                                headers=auth_token_header)
                data2 = res.json()
                print(f"Printing data2 : {data2}")
                campaigns = data2['campaigns']
                metrics = item["metrics"]
                for day, impressions in metrics.items():
                    raw_impressions = impressions['rawImpressions']
                    validated_impressions = impressions['validatedImpressions']
                    for campaign in campaigns:
                        data_set = merge_func(
                            item, campaign, raw_impressions, validated_impressions, day)
                    final_response.append(data_set)
    generate_csv(CSV_FILE, final_response)  # write to csv file
    # print(final_response)
    # log_csv_data(CSV_FILE)


    upload_file_to_s3(  CSV_FILE, BUCKET_NAME,  report_name)  

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": str(final_response)
    }


def merge_func(item, campaign,  raw_impressions, validated_impressions, day=None):
    if campaign['id'] == item['id']:
        data_set = {
            "Campaign ID": item["id"],
            "Campaign Name": campaign["name"],
            # "contentProvider": campaign["contentProvider"],
            # "status": campaign["status"],
            # "type": campaign["type"],
            # "startTime": campaign["startTime"],
            # "endTime": campaign["endTime"],
            # "priority": campaign["priority"],
            # "notes": campaign["notes"],
            # "advertiser": campaign["advertiser"],
            # "addExec": campaign["addExec"],
            "Date": day,
            "Raw Impressions": raw_impressions,
            # "validated_impressions": validated_impressions,
        }
        if day is None:
            data_set.pop("date")

    return data_set


def generate_csv(file, data):
    # print(type(data))
    data_file = open(file, 'w')
    csv_writer = csv.writer(data_file)
    count = 0
    for  item in data:
        # print(item)
        if count == 0:
            header = item.keys()
            csv_writer.writerow(header)
            count += 1
        csv_writer.writerow(item.values())
    data_file.close()     

def upload_file_to_s3(file , bucket_name,  report_name):
    try:
        s3_client = boto3.client('s3')
        s3_client.upload_file(file, bucket_name, report_name)
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise e from e
    return True

def log_csv_data(csv_file):
    with open(csv_file) as file:
        csvreader = csv.reader(file)
        # header = next(csvreader)

        # print(f"Printing header values : {header}")
        rows = list(csvreader)
        print(f"Printing rows : {rows}")    
