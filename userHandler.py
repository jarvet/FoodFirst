import boto3


def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('user')
    type = event['messages'][0]['type']
    userinfo = event['messages'][0]['unstructured']
    check_user_info(userinfo)
    username = userinfo['username']

    if type == "get":
        if not exist(table, username):
            put(table, userinfo)

    if type == "update":
        update(table, userinfo)

    if type == 'newuser':
        if exist(table, username):
            result = {
                "messages": [
                    {
                        "type": "fail",
                        "unstructured": {"error": "user already exist"}
                    }
                ]
            }
            return result
        put(table, userinfo)

    response = table.get_item(Key={"username": username})
    response["Item"]["prefer"] = [] if response["Item"]["prefer"] is None or response["Item"]["prefer"] == " " or \
                                       response["Item"]["prefer"] == "" else response["Item"]["prefer"].split(",")
    return_user_info(response["Item"])
    result = {
        "messages": [
            {
                "type": "success",
                "unstructured": response["Item"]
            }
        ]
    }

    return result


def update(table, userinfo):
    username = userinfo['username']
    fullname = userinfo['fullname']
    email = userinfo['email']
    phone = userinfo['phone']
    address = userinfo['address']
    zip = userinfo['zip']
    prefer = userinfo['prefer']

    if exist(table, username):
        table.update_item(
            Key={"username": username},
            UpdateExpression="set fullname=:f, email=:e, phone=:p, address=:a, zip=:z, prefer=:l",
            ExpressionAttributeValues={
                ':f': fullname,
                ':e': email,
                ':p': phone,
                ':a': address,
                ':z': zip,
                ':l': prefer
            },
            # ReturnValues="UPDATED_NEW"
        )
    else:
        put(table, userinfo)


def put(table, userinfo):
    table.put_item(
        Item=userinfo
    )


def exist(table, username):
    response = table.get_item(Key={"username": username})
    if "Item" not in response:
        return False
    return True


def check_user_info(userinfo):
    username = userinfo['username']
    userinfo['fullname'] = " " if userinfo['fullname'] is None or userinfo['fullname'] == "" else userinfo['fullname']
    userinfo['email'] = " " if userinfo['email'] is None or userinfo['email'] == "" else userinfo['email']
    userinfo['phone'] = " " if userinfo['phone'] is None or userinfo['phone'] == "" else userinfo['phone']
    userinfo['address'] = " " if userinfo['address'] is None or userinfo['address'] == "" else userinfo['address']
    userinfo['zip'] = " " if userinfo['zip'] is None or userinfo['zip'] == "" else userinfo['zip']
    userinfo['prefer'] = " " if userinfo['prefer'] is None or userinfo['prefer'] == "" else userinfo['prefer']


def return_user_info(userinfo):
    username = userinfo['username']
    userinfo['fullname'] = "" if userinfo['fullname'] is None or userinfo['fullname'] == " " else userinfo['fullname']
    userinfo['email'] = "" if userinfo['email'] is None or userinfo['email'] == " " else userinfo['email']
    userinfo['phone'] = "" if userinfo['phone'] is None or userinfo['phone'] == " " else userinfo['phone']
    userinfo['address'] = "" if userinfo['address'] is None or userinfo['address'] == " " else userinfo['address']
    userinfo['zip'] = "" if userinfo['zip'] is None or userinfo['zip'] == " " else userinfo['zip']
    userinfo['prefer'] = "" if userinfo['prefer'] is None or userinfo['prefer'] == " " else userinfo['prefer']