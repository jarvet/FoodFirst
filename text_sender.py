import logging
import os
import time

import boto3

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def format_suggestion(no, business):
    return '\r\n{}. {}, Rating: {}, located at {}'.format(no, business['Name'], business['Rating'],
                                                          business['Address'].strip())


def deal_message(message):
    attributes = message['MessageAttributes']
    name = attributes['name']['StringValue']
    address = attributes['address']['StringValue']
    display_phone = attributes['display_phone']['StringValue']
    to_phone = attributes['to_phone']['StringValue']

    sns = boto3.client("sns")
    message_content = '[FoodFirst Reminder] The restaurant you are interested in is:\n' \
                      '{}, at {}, {}.\nEnjoy your meal~'.format(name, address, display_phone)

    sns.publish(
        PhoneNumber='+1' + to_phone,
        Message=message_content
    )


def receive_and_delete():
    # create SQS client
    sqs = boto3.client('sqs')

    queue_url = 'https://sqs.us-east-1.amazonaws.com/730687685526/ccproject_text_message'

    # Receive message from SQS queue
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )
    if 'Messages' not in response:
        return
    for message in response['Messages']:
        receipt_handle = message['ReceiptHandle']
        deal_message(message)

        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )


def lambda_handler(event, context):
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    receive_and_delete()
    # return dispatch(event)
