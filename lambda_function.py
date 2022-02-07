# import pandas as pd

def lambda_handler(event, context):

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": "Hello TATA ELXSI!" 
        }
    