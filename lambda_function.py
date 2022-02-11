# import pandas as pd
import requests, json

def lambda_handler(event, context):
    URL1 = "https://mediahub.invidi.it/api/impressions/v1/timeseries/campaigns"
    URL2 = "https://mediahub.invidi.it/api/campaign-management/v1/campaigns"
    auth_token = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IlBuUTN4Sl8ySWU1T0YzdFJQVG1qMiJ9.eyJodHRwczovL21lZGlhaHViLmludmlkaS5pdC9wcmluY2lwYWwiOnsiZGlzcGxheV9uYW1lIjoiS2V0aGFyZWVzd2FyYW4gS3Jpc2huYW4iLCJlbWFpbCI6ImtldGhhcmVlc3dhcmFua0B0YXRhZWx4c2kuY28uaW4iLCJpZCI6ImFmODVjMzMyOGQxZmVkODY3OGIxZmM2NDQxMmVjMTBiMDllYzE0MzA2ZDk0YzgxM2IwYTU1ODkxMzljMmYzZTkiLCJ0eXBlIjoidXNlciJ9LCJodHRwczovL21lZGlhaHViLmludmlkaS5pdC9wZXJtaXNzaW9ucyI6WyJpbnZlbnRvcnk6RGVsZXRlV2luZG93IiwiaW1wcmVzc2lvbnM6Q3JlYXRlUmF3SW1wcmVzc2lvbkZpbGUiLCJtZWRpYWh1YjpSZWFkQWxsIiwidGFyZ2V0aW5nOlVwZGF0ZU1hcHBpbmdzIiwiaW1wcmVzc2lvbnM6UmVhZEFnZ3JlZ2F0ZUltcHJlc3Npb25Db3VudHMiLCJpbXByZXNzaW9uczpEZWxldGVSYXdJbXByZXNzaW9uRmlsZSIsInRhcmdldGluZzpSZWFkQXR0cmlidXRlcyIsImludmVudG9yeTpDcmVhdGVXaW5kb3ciLCJ0YXJnZXRpbmc6RGVsZXRlQXR0cmlidXRlcyIsIm1lZGlhaHViOldyaXRlQWxsIiwiaW52ZW50b3J5OkRlbGV0ZUFsbG9jYXRpb24iLCJ0YXJnZXRpbmc6RGVsZXRlTWFwcGluZ3MiLCJjYW1wYWlnbi1tYXBwaW5nczpSZWFkQ2FtcGFpZ25NYXBwaW5ncyIsImludmVudG9yeTpEZWxldGVBdmFpbCIsImludmVudG9yeTpVcGRhdGVXaW5kb3ciLCJ0YXJnZXRpbmc6U2VhcmNoTWFwcGluZ3MiLCJpbnZlbnRvcnk6Q3JlYXRlU2xvdCIsImltcHJlc3Npb25zOlVwZGF0ZVJhd0ltcHJlc3Npb25GaWxlIiwiaW52ZW50b3J5OkNyZWF0ZUF2YWlsIiwidGFyZ2V0aW5nOlNlYXJjaEF0dHJpYnV0ZXMiLCJ0YXJnZXRpbmc6UmVhZE1hcHBpbmdzIiwidGFyZ2V0aW5nOkNyZWF0ZUF0dHJpYnV0ZXMiLCJpbnZlbnRvcnk6RGVsZXRlU2xvdCIsInRhcmdldGluZzpVcGRhdGVBdHRyaWJ1dGVzIiwiaW52ZW50b3J5OkNyZWF0ZUFsbG9jYXRpb24iXSwiaHR0cHM6Ly9sb2dpbi5pbnZpZGktZWRnZS5jb20vYXBwX21ldGFkYXRhIjp7InByb3ZpZGVyIjoiOTA1ZDk0MDEtZTJkMy00YjcyLTkzOWYtMzY5NjY4MzU0NTUyIn0sImlzcyI6Imh0dHBzOi8vbG9naW4uaW52aWRpLml0LyIsInN1YiI6IndhYWR8M19XdGs0SDBUZy13Tl9TUmc5aTdaa3ZLZUhVX0lVdDdwMmkwVjlvQlVXVSIsImF1ZCI6WyJodHRwczovL21lZGlhaHViLmludmlkaS5pdCIsImh0dHBzOi8vaW52aWRpLWludGVncmF0aW9uLnVzLmF1dGgwLmNvbS91c2VyaW5mbyJdLCJpYXQiOjE2NDQ0NzExMzgsImV4cCI6MTY0NDU1NzUzOCwiYXpwIjoidDVxd3FkekNnU090Wm9BSmtCQm9JT3hlZkl2cU9lM1UiLCJzY29wZSI6Im9wZW5pZCBwcm9maWxlIGVtYWlsIG9mZmxpbmVfYWNjZXNzIHByb3ZpZGVyOjkwNWQ5NDAxLWUyZDMtNGI3Mi05MzlmLTM2OTY2ODM1NDU1MiJ9.guDIcgR1ZHVg67hgBop-mhXH0-qs6NWz5Ai_IdZm19dM6NV3Ap2gXdIlk3R-D4DaZdCOIO41TQwNhP2kkrarmHto0IxjKGbiI-93Pzxu-pTIAypaeADrlt35scovJQIdX9ffiwQtVxl9ikx7qUa2q8U0bWG3ijYfzjTT6FzGVVLeui_vlRKyW-bKOPBMeCbPDsNP-heO6nZFppYFct0FaW1Etxeo1SKq5XruzgWzqNWEpMctWYDNblkE_R8WQYXWY724A9jjHKTJNkgbTnk4HZDiRtLzyLDjg1VDoYXyJY8JOF7W_nTMN_jAG0XedvqxtOeE7stfegYt7U3A9050UQ'
    auth_token_header_value = f'Bearer {auth_token}'
    auth_token_header = {"Authorization": auth_token_header_value}

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
            # pay_load = {"id":"0731ba4d-5b35-42aa-add7-6066d92b35b2"}
            # res = requests.get(url,params=pay_load, headers=auth_token_header)
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
    