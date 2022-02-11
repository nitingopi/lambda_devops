# import pandas as pd
import requests, json

def lambda_handler(event, context):
    startDate = None
    endDate = None
    reportName = None

    print('FROM_DATE = ' + str(event['FROM_DATE']))
    startDate = str(event['FROM_DATE'])

    print('TO_DATE = ' + str(event['TO_DATE']))
    endDate = str(event['TO_DATE'])

    print('REPORT_NAME = ' + str(event['REPORT_NAME']))
    reportName = str(event['REPORT_NAME'])

    print('DIMENSION = ' + str(event['DIMENSION'])) # no information how to use
    print('METRICS = ' + str(event['METRICS'])) # no information how to use

    print('TOKEN = ' + str(event['TOKEN']))
    auth_token = str(event['TOKEN'])

    URL1 = "https://mediahub.invidi.it/api/impressions/v1/timeseries/campaigns"
    URL2 = "https://mediahub.invidi.it/api/campaign-management/v1/campaigns"
    auth_token_header_value = f'Bearer {auth_token}'
    auth_token_header = {"Authorization": auth_token_header_value}

    if startDate is not None and endDate is not None:
        print('startDate and endDate are not None')
        URL1_params = "https://mediahub.invidi.it/api/impressions/v1/totals/campaigns"

        URL1 = f'{URL1_params}?startDate={startDate}&endDate={endDate}'
               

            

    # call API1 and get the list of campaign_id
    r1 = requests.get(url=URL1, headers=auth_token_header)

    # extracting data in json format
    data1 = r1.json()



    # return final_data
    final_response = []

    for item in data1:
        if item["id"] and item["id"] != None:
            pay_load = {"id":item["id"]}
            # call API2 corresponding to the campaign_id

            res = requests.get(URL2, params=pay_load, headers=auth_token_header)
            data2 = res.json() 
            # return data2   
            metrics = item["metrics"]
            metrics_keys = metrics.keys()
            # Merge both data
            campaigns = data2['campaigns']
            for day in metrics_keys:
                for campaign in campaigns:
                    # print(campaign)
                    # return campaign
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
                            "raw_impressions": metrics[day]["rawImpressions"],
                            "validated_impressions": metrics[day][
                                "validatedImpressions"
                            ],
                        }
                final_response.append(data_set)

    # return str(final_response)

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": str(final_response) 
        }
    