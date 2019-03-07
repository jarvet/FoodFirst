import json
import logging
from urllib.error import HTTPError

import boto3
import certifi
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

ES_HOST = 'https://search-restaurants-ehyxkephvc7jtvvwhkpratt2p4.us-east-1.es.amazonaws.com/'
awsauth = AWS4Auth("********************", "****************************************", "us-east-1", "es")


def query_es(terms, rating, item_num=20):
    es = Elasticsearch([ES_HOST], port=9200, http_auth=awsauth, use_ssl=True, ca_certs=certifi.where(),
                       connection_class=RequestsHttpConnection)
    query_body = {
        "from": 0, "size": 20,
        "query": {
            "bool": {
                "must": {
                    "multi_match": {
                        "query": terms,
                        "type": "cross_fields",
                        "fields": ["name", "term", "price"]
                    }
                },
                "filter": [
                    {
                        "range": {
                            "rating": {"gte": rating}
                        }
                    }
                ]
            },
        }
    }
    res = es.search(index='restaurants', doc_type='Restaurant', body=query_body)
    sources = list(map(lambda x: x['_source'], res['hits']['hits']))
    id_list = []
    for source in sources[:item_num]:
        id_list.append(source['id'])
    return id_list


def query_dynamodb(id):
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('ccproject-restaurants')

    try:
        response = table.query(
            KeyConditionExpression=Key('id').eq(id)
        )
        item = response['Items'][0]
        item['location'] = eval(item['location'])
        item['transactions'] = eval(item['transactions'])
        item['categories'] = eval(item['categories'])
        item['coordinates'] = eval(item['coordinates'])

    except ClientError as e:
        logger.debug(e.response['Error']['Message'])
    else:
        return item


def pack_message(unstructured):
    # return {
    #     'type':'string',
    #     'unstructured':unstructured
    # }
    return unstructured


def send_message(unstructured_message):
    id = unstructured_message['keywords']
    to_phone = unstructured_message['phone']
    business = query_dynamodb(id)

    # create SQS client
    sqs = boto3.client('sqs')

    queue_url = 'https://sqs.us-east-1.amazonaws.com/730687685526/ccproject_text_message'

    # Send message to SQS queue
    response = sqs.send_message(
        QueueUrl=queue_url,
        DelaySeconds=10,
        MessageAttributes={
            'name': {'StringValue': business['name'], 'DataType': 'String'},
            'address': {'StringValue': ",".join(business['location']['display_address']), 'DataType': 'String'},
            'display_phone': {'StringValue': business['display_phone'], 'DataType': 'String'},
            'to_phone': {'StringValue': to_phone, 'DataType': 'String'}
        },
        MessageBody='message from LF1'
    )

    return pack_message("message pushed!")


def search_list(unstructured_message):
    keywords = unstructured_message['keywords'].split(',')[:-1]
    rating = unstructured_message['keywords'].split(',')[-1]
    keywords = '(' + ') OR ('.join(keywords) + ')'
    try:
        id_list = query_es(keywords, rating)
        businesses = list(map(query_dynamodb, id_list))
        re_message = pack_message(businesses)
    except HTTPError as error:
        logger.debug(
            'Encountered HTTP error {0} on {1}:\n {2}\nAbort program.'.format(
                error.code,
                error.url,
                error.read(),
            )
        )
    else:
        return re_message


def log_click(unstructured_message):
    id = unstructured_message['keywords']
    item = {
        "REST_ID": id,
        "USER_ID": "123456"
    }
    client = boto3.client('kinesis')
    s = json.dumps(item)
    enc = s.encode()
    response = client.put_record(
        StreamName='ccproject_click_log',
        Data=enc,
        PartitionKey='1'
    )

    return pack_message("click logged")


def log_query():
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('ccproject-restaurants')

    try:
        id = 'click_count_top5'
        response = table.query(
            KeyConditionExpression=Key('id').eq(id)
        )
        print(response)
        item = response['Items'][0]
        top5 = eval(item['top5'])
        id_list = [eval(item)['ITEM'] for item in top5]
        businesses = list(map(query_dynamodb, id_list))
        re_message = pack_message(businesses)
    except:
        return []
    else:
        return re_message


def lambda_handler(event, context):
    messages = event['messages']
    re_messages = []
    for message in messages:
        unstructured_message = message['unstructured']
        searchtype = unstructured_message['searchtype']
        if searchtype == 'sendtext':
            re_messages.append(send_message(unstructured_message))
        elif searchtype == 'clicklog':
            re_messages.append(log_click(unstructured_message))
        elif searchtype == 'logquery':
            re_messages += log_query()
        else:
            re_messages += search_list(unstructured_message)

    return re_messages

# 
# a = lambda_handler({
#     'messages': [{
#         'type': 'string',
#         'unstructured': {
#             'keywords': 'Chinese,0',
#             'searchtype': 'normal',
#             'tag1': '6469542609'
#         }
#     }]
# }, '')
