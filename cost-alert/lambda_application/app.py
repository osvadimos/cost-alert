import json

# import requests
from lambda_function import read_s3_file, send_slack_message, read_last_100, analyze_w_last100, \
    upload_response_list_to_s3


def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    send_slack_message(message='New get_cost request', msg_type=0)
    file_from_s3 = read_s3_file()
    list_of_100 = read_last_100(file_from_s3, 100)
    analyzed_list_of_100 = analyze_w_last100(list_of_100)
    upload_response_list_to_s3(analyzed_list_of_100)

    return {
        'statusCode': 200,
        'body': json.dumps('Checking costs')
    }
