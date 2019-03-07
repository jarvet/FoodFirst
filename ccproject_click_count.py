# import json
import base64
import json

import boto3


def connect_table(top5):
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('ccproject-restaurants')

    item = {
        "id": "click_count_top5",
        "name": "top5",
        "top5": top5
    }
    table.put_item(
        Item=item
    )


def lambda_handler(event, context):
    top5 = []
    for record in event['records']:
        # Kinesis data is base64 encoded so decode here
        item = json.loads(base64.b64decode(record["data"]))
        top5.append(str(item))
    connect_table(str(top5))
