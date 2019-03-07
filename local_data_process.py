from collections import defaultdict

import requests
import urllib
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.parse import urlencode
import csv
import boto3
import time
import json

API_KEY = '**********************************************'
# API constants, you shouldn't have to change these.
API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'  # Business ID will come after slash.
SEARCH_LIMIT = 50
TARGET_NUM = 1000


def request(host, path, api_key, url_params=None):
    """Given your API_KEY, send a GET request to the API.

    Args:
        host (str): The domain host of the API.
        path (str): The path of the API after the domain.
        API_KEY (str): Your API Key.
        url_params (dict): An optional set of query parameters in the request.

    Returns:
        dict: The JSON response from the request.

    Raises:
        HTTPError: An error occurs from the HTTP request.
    """
    url_params = url_params or {}
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }

    print(u'Querying {0} ...'.format(url))

    response = requests.request('GET', url, headers=headers, params=url_params)

    return response.json()


def search(api_key, term, location, offset):
    """Query the Search API by a search term and location.

    Args:
        term (str): The search term passed to the API.
        location (str): The search location passed to the API.

    Returns:
        dict: The JSON response from the request.
    """

    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        'limit': SEARCH_LIMIT,
        'offset': offset
    }
    return request(API_HOST, SEARCH_PATH, api_key, url_params=url_params)



def format_content_dynamo(term, business):
    return {
        "BusinessId": business['id'],
        "Name": business['name'],
        "Address": ", ".join(business['location']['display_address']),
        "Coordinates": "({0},{1})".format(business['coordinates']['latitude'], business['coordinates']['longitude']),
        "NumberOfReviews": business['review_count'],
        "Rating": business['rating'],
        "ZipCode": business['location']['zip_code']
    }

def format_content_file1(term, business):
    return {
        "BusinessId": business['id'],
        "Cusine": term,
        "NumberOfReviews": business['review_count'],
        "Rating": business['rating'],
    }


def query_api(term, location, offset):
    """Queries the API by the input values from the user.

    Args:
        term (str): The search term to query.
        location (str): The location of the business to query.
    """

    response = search(API_KEY, term, location, offset)
    print(response.get('total'))
    businesses = response.get('businesses')
    # dynamo_list = []
    file1_list = []
    if not businesses:
        print(
            u'No businesses for {0} in {1} found.'.format(term, location))
        return []

    for business in businesses:
        # dynamo_list.append(format_content_dynamo(term, business))
        if 'term' not in business:
            business['term'] = term
        file1_list.append(business)
    # return dynamo_list, file1_list
    return file1_list

def get_restaurants_list(cuisine, location):
    call_times = 0
    # dynamo_list = []
    file1_list = []
    while call_times * SEARCH_LIMIT <= TARGET_NUM:
        try:
            # query_dynamo, query_file1 = query_api(cuisine, location, call_times * SEARCH_LIMIT)
            # dynamo_list += query_dynamo
            # file1_list += query_file1
            file1_list += query_api(cuisine, location, call_times * SEARCH_LIMIT)
            call_times += 1
        except HTTPError as error:
            print(
                'Encountered HTTP error {0} on {1}:\n {2}\nAbort program.'.format(
                    error.code,
                    error.url,
                    error.read(),
                )
            )
    # return dynamo_list, file1_list
    return file1_list

def write_to_file(file, restaurants_list):
    with open(file, 'w', encoding='utf-8', newline='') as csvfile:
        dict_writer = csv.DictWriter(csvfile, restaurants_list[0].keys())
        dict_writer.writeheader()
        for row in restaurants_list:
            try:
                dict_writer.writerow(row)
            except UnicodeEncodeError:
                print(row['Name'])
                row['Name'] = '?????'


def read_from_file(file):
    csv_rows = []
    with open(file, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        title = reader.fieldnames
        for row in reader:
            csv_rows.extend([{title[i]: row[title[i]] for i in range(len(title))}])
    return csv_rows


def create_dynamodb():
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.create_table(
        TableName='ccproject-restaurants',
        KeySchema=[
            {
                'AttributeName': 'id',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'name',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'id',
                'AttributeType': 'S'  # Partition key
            },
            {
                'AttributeName': 'name',
                'AttributeType': 'S'  # Sort key
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    print("Table status:", table.table_status)


def connect_table():
    restaurant_list = read_from_file("complete_data.csv")

    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('ccproject-restaurants')

    for restaurant in restaurant_list:
        item = {}
        for key, value in restaurant.items():
            if value and key!='distance':
                item[key] = value
        table.put_item(
            Item=item
        )

def convert_to_json(input_file, output_file):
    # curl -XPOST https://search-restaurants-ehyxkephvc7jtvvwhkpratt2p4.us-east-1.es.amazonaws.com/_bulk --data-binary @list_for_es.json -H 'Content-Type: application/json'

    restaurants_list = read_from_file(input_file)
    i = 0
    def index_format(num):
        return { "index" : { "_index": "restaurants", "_type" : "Restaurant", "_id" : str(num) } }

    with open(output_file, 'w', encoding='utf-8') as jsonfile:
        for row in restaurants_list:
            item = {
                'id': row['id'],
                'name': row['name'],
                'rating': row['rating'],
                'price': row['price'],
                'term': row['term']
            }
            jsonfile.write(json.dumps(index_format(i)))
            jsonfile.write('\n')
            i+=1
            jsonfile.write(json.dumps(item))
            jsonfile.write('\n')


def merge_terms():
    list_with_terms = read_from_file("complete_data_with_terms.csv")
    restaurant_list = read_from_file("complete_data.csv")
    term_dict = defaultdict(list)
    for row in list_with_terms:
        term_dict[row["id"]].append(row["term"])
    i = j = 0
    new_list = []
    for row in restaurant_list:
        if row["id"] in term_dict:
            row["term"] = term_dict[row["id"]]
            new_list.append(row)
            i+=1
            del term_dict[row["id"]]
        else:
            j+=1
    print(i)
    print(j)
    write_to_file("list_merged_term.csv", new_list)


def get_file_for_es():
    # curl -XPOST https://search-restaurants-ehyxkephvc7jtvvwhkpratt2p4.us-east-1.es.amazonaws.com/_bulk --data-binary @list_for_es_learned.json -H 'Content-Type: application/json'

    restaurant_list = read_from_file("list_after_learning.csv")
    new_list = []
    for row in restaurant_list:
        if not row['recommand'] or row['recommand']==0 or row['recommand']=='0':
            print(row)
            continue
        item = {
            'id':row['id'],
            'name':row['name'],
            'rating':row['rating'],
            'price':row['price'],
            'term':row['term']
        }
        new_list.append(item)
    write_to_file("list_for_es_learned.csv",new_list)
    convert_to_json('list_for_es_learned.csv','list_for_es_learned.json')


def json_to_es_format(input_file, output_file):
    # curl -XPOST https://search-restaurants-ehyxkephvc7jtvvwhkpratt2p4.us-east-1.es.amazonaws.com/_bulk --data-binary @to_es.json -H 'Content-Type: application/json'

    # restaurants_list = read_from_file(input_file)
    with open(input_file, 'r', encoding='utf-8') as jsonfile:
        restaurants_list = json.load(jsonfile)
    i = 0
    def index_format(num):
        return { "index" : { "_index": "restaurants", "_type" : "Restaurant", "_id" : str(num) } }

    with open(output_file, 'w', encoding='utf-8') as jsonfile:
        for row in restaurants_list:
            if not row['recommand']:
                continue
            item = {
                'id': row['id'],
                'name': row['name'],
                'rating': row['rating'],
                'price': row['price'],
                'term': row['term']
            }
            jsonfile.write(json.dumps(index_format(i)))
            jsonfile.write('\n')
            i+=1
            jsonfile.write(json.dumps(item))
            jsonfile.write('\n')

if __name__ == '__main__':
    file_list = []
    for cuisine in {"Chinese", "American", "Japanese", "Korean", "Italian", "Indian","Thai","Britain","Spicy","Sweet","Grill","Breakfast","Lunch","brunch","dinner"}:
        file_list += get_restaurants_list(cuisine, 'Manhattan')
    write_to_file('complete_data_with_terms.csv',file_list)
    merge_terms()
    get_file_for_es()

    # create_dynamodb()
    # connect_table()

    # json_to_es_format('list_after_learning.json','to_es.json')

    # get_file_for_es()