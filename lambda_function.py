# import pandas as pd
import requests
import json


def lambda_handler(event, context):
    startDate = None
    endDate = None
    reportName = None

    if 'FROM_DATE' in event:
        print('FROM_DATE = ' + str(event['FROM_DATE']))
        startDate = str(event['FROM_DATE'])

    if 'TO_DATE' in event:
        print('TO_DATE = ' + str(event['TO_DATE']))
        endDate = str(event['TO_DATE'])

    if 'REPORT_NAME' in event:
        print('REPORT_NAME = ' + str(event['REPORT_NAME']))
        reportName = str(event['REPORT_NAME'])

    if 'DIMENSION' in event:
        # no information how to use
        print('DIMENSION = ' + str(event['DIMENSION']))

    if 'METRICS' in event:
        # no information how to use
        print('METRICS = ' + str(event['METRICS']))

    if 'TOKEN' in event:
        print('TOKEN = ' + str(event['TOKEN']))
        auth_token = str(event['TOKEN'])

    URL1 = "https://mediahub.invidi.it/api/impressions/v1/timeseries/campaigns"
    URL2 = "https://mediahub.invidi.it/api/campaign-management/v1/campaigns"
    auth_token_header_value = f'Bearer {auth_token}'
    auth_token_header = {"Authorization": auth_token_header_value}

    if startDate is not None and endDate is not None:
        print('startDate and endDate are not None')
        # URL1_params = "https://mediahub.invidi.it/api/impressions/v1/timeseries/campaigns"

        URL1 = f'{URL1}?startDate={startDate}&endDate={endDate}'

    # call API1 and get the list of campaign_id
    r1 = requests.get(url=URL1, headers=auth_token_header)

    # extracting data in json format
    data1 = r1.json()

    # return final_data
    final_response = []

    for item in data1:
        if item["id"] and item["id"] != None:
            pay_load = {"id": item["id"]}
            # call API2 corresponding to the campaign_id
            res = requests.get(URL2, params=pay_load,
                               headers=auth_token_header)
            data2 = res.json()
            campaigns = data2['campaigns']
            if startDate is not None and endDate is not None:
                raw_impressions = item['metrics']['rawImpressions']
                validated_impressions = item['metrics']['validatedImpressions']
                for campaign in campaigns:
                    data_set = merge_func(
                        item, campaign, raw_impressions, validated_impressions)
                final_response.append(data_set)
            else:
                metrics = item["metrics"]
                for day, impressions in metrics.items():
                    raw_impressions = impressions['rawImpressions']
                    validated_impressions = impressions['validatedImpressions']
                    for campaign in campaigns:
                        data_set = merge_func(
                            item, campaign, raw_impressions, validated_impressions, day)
                    final_response.append(data_set)

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
            "ID": item["id"],
            "name": campaign["name"],
            "contentProvider": campaign["contentProvider"],
            "status": campaign["status"],
            "type": campaign["type"],
            "startTime": campaign["startTime"],
            "endTime": campaign["endTime"],
            "priority": campaign["priority"],
            # "notes": campaign["notes"],
            "advertiser": campaign["advertiser"],
            # "addExec": campaign["addExec"],
            "date": day,
            "raw_impressions": raw_impressions,
            "validated_impressions": validated_impressions,
        }
        if day is None:
            data_set.pop("date")

    return data_set
