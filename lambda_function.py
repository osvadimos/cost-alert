import json
import boto3
import requests
from datetime import datetime, timedelta


def lambda_handler(event, context):
    client = boto3.client('ce')

    # todo explore costs
    # todo
    send_slack_message("tst python message")
    # todo
    from datetime import datetime, timedelta
    now_date = datetime.now()
    now_date = now_date - timedelta(hours=72) - timedelta(hours=72)
    previous_date = now_date - timedelta(hours=72)
    now_date_string = now_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    previous_date_string = previous_date.strftime("%Y-%m-%dT%H:%M:%SZ")

    previous_date_string = "2020-09-25"
    now_date_string = "2020-09-30"

    response = client.get_cost_and_usage(TimePeriod={
        'Start': f'{previous_date_string}',
        'End': f'{now_date_string}'
    }, Granularity='DAILY',
        Metrics=['NetAmortizedCost'])

    print(response)

    return {
        'statusCode': 200,
        'body': json.dumps('Checking costs')
    }


def send_slack_message(message):
    slack_url = "https://hooks.slack.com/services/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"

    message_text = f"{message} <@UJBAP0YJJ>"

    data = {'text': message_text}
    requests.post(url=slack_url, data=json.dumps(data))
    print("Send message to slack")
