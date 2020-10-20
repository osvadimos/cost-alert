import json
import os
from datetime import datetime, timedelta

import boto3
import pandas as pd
import requests
import smtplib
from email.mime.text import MIMEText


def read_s3_file():
    session = boto3.Session(
        aws_access_key_id=os.environ['ACCESS'],
        aws_secret_access_key=os.environ['SECRET']
    )
    s3 = session.resource('s3')
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.environ['ACCESS'],
        aws_secret_access_key=os.environ['SECRET'],
        region_name=os.environ['REGION_NAME']
    )
    s3_key = os.environ['S3_KEY']
    s3_bucket = os.environ['S3_BUCKET']
    obj = s3.Object(s3_bucket, s3_key)
    if key_exists(s3_client, s3_key, s3_bucket):
        from_s3 = obj.get()['Body'].read().decode('utf-8')
        print('downloaded file from s3')
        return from_s3
    else:
        print(f"Could not find a file in S3.")


def key_exists(s3_client, mykey, mybucket):
    response = s3_client.list_objects_v2(Bucket=mybucket, Prefix=mykey)
    if response:
        for obj in response['Contents']:
            if mykey == obj['Key']:
                return True
    return False


def read_last_100(prev_responses, start_date):
    client = boto3.client(
        'ce',
        aws_access_key_id=os.environ['ACCESS'],
        aws_secret_access_key=os.environ['SECRET'],
        region_name=os.environ['REGION_NAME']
    )

    now_date = datetime.now()
    now_date_string = now_date.strftime("%Y-%m-%d")  # TODAY

    input_start_date = datetime.strptime(start_date, "%Y-%m-%d")  # '2020-10-18'
    today_date = datetime.strptime(now_date_string, "%Y-%m-%d")
    delta = abs((today_date - input_start_date).days)

    if len(prev_responses) < 100:  # by default should be == 100
        if delta < 100:
            previous_date = now_date - delta * timedelta(hours=24)
            previous_date_string = previous_date.strftime("%Y-%m-%d")

            response = client.get_cost_and_usage(TimePeriod={
                'Start': f'{previous_date_string}',
                'End': f'{now_date_string}'
            }, Granularity='DAILY',
                Metrics=['NetAmortizedCost'])

            new_prev_responses = json.loads(str(json.dumps(response)))['ResultsByTime']
            print(len(new_prev_responses))
            print('new response=', new_prev_responses)
            return new_prev_responses
        else:
            previous_date = now_date - 100 * timedelta(hours=24)
            previous_date_string = previous_date.strftime("%Y-%m-%d")

            response = client.get_cost_and_usage(TimePeriod={
                'Start': f'{previous_date_string}',
                'End': f'{now_date_string}'
            }, Granularity='DAILY',
                Metrics=['NetAmortizedCost'])

            new_prev_responses = json.loads(str(json.dumps(response)))['ResultsByTime']
            print('len of new response =', len(new_prev_responses))
            print('new response =', new_prev_responses)
            return new_prev_responses
    else:
        previous_date = now_date - timedelta(hours=24)
        previous_date_string = previous_date.strftime("%Y-%m-%d")

        response = client.get_cost_and_usage(TimePeriod={
            'Start': f'{previous_date_string}',
            'End': f'{now_date_string}'
        }, Granularity='DAILY',
            Metrics=['NetAmortizedCost'])

        resp = json.loads(str(json.dumps(response)))['ResultsByTime'][0]
        print('new response =', resp)
        print('prev resp =', prev_responses)
        new_prev_responses = json.loads(prev_responses)
        if resp['TimePeriod']['Start'] == new_prev_responses[-1]['TimePeriod']['Start'] and \
                resp['TimePeriod']['End'] == new_prev_responses[-1]['TimePeriod']['End']:
            new_prev_responses[-1] = resp
            print('Last record is updated')
            print('len of updated response =', len(new_prev_responses))
            return new_prev_responses
        else:
            new_prev_responses.append(resp)
            print('after append', new_prev_responses)
            del new_prev_responses[0]
            print('len of new_previous =', len(new_prev_responses))
            print('edited response =', new_prev_responses)
            return new_prev_responses


def analyze_w_last100(last_100_list):
    df_start = pd.DataFrame((obj['TimePeriod']['Start'],
                             obj['Total']['NetAmortizedCost']['Amount']) for obj in last_100_list)

    df_start.columns = ['start', 'amount']
    df_start = df_start.astype({'amount': 'float64'})
    last_response = df_start.loc[int(df_start.shape[0]) - 1]
    last_response_month = str(df_start.iloc[int(df_start.shape[0] - 1)]['start'][0:-3])

    df_start_first_only = df_start[df_start['start'].str.endswith('01')].reset_index(drop=True)
    avg_start_only = df_start_first_only.iloc[:-1]['amount'].mean()

    df_not_first = df_start[~df_start['start'].str.endswith('01')].reset_index(drop=True)
    avg_not_start = df_not_first.iloc[:-1]['amount'].mean()

    last_response_amount = last_response['amount']

    if last_response['start'].endswith('01') is True:
        if df_start_first_only.loc[int(df_start_first_only.shape[0] - 1)]['amount']:
            today_date = str(df_start_first_only.loc[int(df_start_first_only.shape[0] - 1)]['start'])
            today_spent = str(round(float(df_start_first_only.loc[int(df_start_first_only.shape[0] - 1)]['amount']),
                                    2))
            prev_date = str(df_start_first_only.loc[int(df_start_first_only.shape[0] - 2)]['start'])
            prev_spent = str(round(float(df_start_first_only.loc[int(df_start_first_only.shape[0] - 2)]['amount']), 2))
            month_mean = round(float(df_start[df_start['start'].str.contains(str(
                df_start.iloc[int(df_start.shape[0]) - 1]['start'][
                0:-3]))].mean()), 2)
            mean_prev_first = str(round(float(avg_start_only), 2))
            at_moment_time = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            len_last_100_first = int(df_start_first_only.shape[0])

            message = f"{at_moment_time} \nNew Expenses request. \n\nYesterday {today_date} spent: {today_spent}" \
                f" USD. (First day of the month) \nIn previous 1st days of month: {prev_date}. Spent: {prev_spent} USD" \
                f"\nMean for this month {last_response_month}: {month_mean} USD" \
                f"\nMean for {len_last_100_first} prev 1st days of months: {mean_prev_first} USD."
            send_slack_message(message, 0)
            send_email_message(message)
            if last_response_amount > avg_start_only * 1.1:
                if last_response_amount > avg_start_only * 1.2:
                    exceed_percent = round(float((last_response_amount / avg_start_only) * 100 - 100), 2)
                    alarm_message = message + f"\n\nAlarm! average begin month daily limit {avg_start_only} USD " \
                        f"exceeded higher 20%, by {exceed_percent}%, now –– {last_response_amount} USD."
                    send_slack_message(alarm_message, 1)
                    send_email_message(alarm_message)
                else:
                    exceed_percent = round(float((last_response_amount / avg_start_only) * 100 - 100), 2)
                    warning_message = message + f"\n\nWarning! average begin month daily limit {avg_start_only} USD " \
                        f"exceeded higher 10%, by {exceed_percent}%, now –– {last_response_amount} USD."
                    send_slack_message(warning_message, 1)
                    send_email_message(warning_message)
    else:
        if df_not_first.loc[int(df_not_first.shape[0] - 1)]['amount']:
            today_date = str(df_not_first.loc[int(df_not_first.shape[0] - 1)]['start'])
            today_spent = str(round(float(df_not_first.loc[int(df_not_first.shape[0] - 1)]['amount']),
                                    2))
            prev_date = str(df_not_first.loc[int(df_not_first.shape[0] - 2)]['start'])
            prev_spent = str(round(float(df_not_first.loc[int(df_not_first.shape[0] - 2)]['amount']), 2))
            month_mean = round(float(df_start[df_start['start'].str.contains(
                str(df_start.iloc[int(df_start.shape[0]) - 1]['start'][0:-3]))].mean()), 2)
            mean_prev_first = str(round(float(avg_start_only), 2))
            mean_not_first = str(round(float(avg_not_start), 2))
            at_moment_time = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            len_last_100_without_first = int(df_not_first.shape[0])
            len_last_100_first = int(df_start_first_only.shape[0])

            message = f"{at_moment_time} \nNew S3 const explorer request. \n\nToday {today_date} spent: {today_spent}" \
                f" USD. (Not first day of the month) \nYesterday: {prev_date}. Spent: {prev_spent} USD" \
                f"\nMean for this month {last_response_month}: {month_mean} USD" \
                f"\nMean for prev {len_last_100_without_first} days (not first days of the month): {mean_not_first} USD" \
                f"\nMean for prev {len_last_100_first} 1st days of months: {mean_prev_first} USD"
            send_slack_message(message, 0)
            send_email_message(message)
            if last_response_amount > avg_not_start * 1.1:
                if last_response_amount > avg_not_start * 1.2:
                    exceed_percent = round(float((last_response_amount / avg_not_start) * 100 - 100), 2)
                    alarm_message = message + f"\n\nAlarm! average daily limit {avg_not_start} USD exceeded by " \
                        f"{exceed_percent}%, now –– {last_response_amount} USD."
                    send_slack_message(alarm_message, 1)
                    send_email_message(alarm_message)
                else:
                    exceed_percent = round(float((last_response_amount / avg_not_start) * 100 - 100), 2)
                    warning_message = message + f"\n\nWarning! average daily limit {avg_not_start} USD exceeded by " \
                        f"{exceed_percent}%, now –– {last_response_amount} USD."
                    send_slack_message(warning_message, 1)
                    send_email_message(warning_message)
    return last_100_list


def send_slack_message(message, msg_type):
    """
    types: 0 - without mention,
    1 - with mention somebody
    """
    try:
        slack_url = os.environ['SLACK_URL']
        slack_id = os.environ['SLACK_ID']

        if msg_type == 0:
            message_text = message
            data = {'text': message_text}
            requests.post(url=slack_url, data=json.dumps(data))
            print("Sent common message to Slack")

        elif msg_type == 1:
            message_text = message + ' <@' + slack_id + '>'
            data = {'text': message_text}
            requests.post(url=slack_url, data=json.dumps(data))
            print("Sent message w/ mention to Slack")
    except:
        print('No environment variable for Slack. Check SLACK_URL, SLACK_ID')


def send_email_message(message):
    try:
        gmail_user = os.environ['GMAIL_USER']
        gmail_password = os.environ['GMAIL_PASSW']
        gmail_to = os.environ['GMAIL_TO']

        fromx = gmail_user
        to = gmail_to
        msg = MIMEText(message)
        msg['Subject'] = 'AWS Cost Report Message'
        msg['From'] = fromx
        msg['To'] = to
        try:
            server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
            server.ehlo()
            server.login(gmail_user, gmail_password)
            server.sendmail(fromx, to, msg.as_string())
            server.quit()
            print('Email sent')
        except:
            print('Something wrong :(')
    except:
        print('No environment variable for GMail. Check GMAIL_USER, GMAIL_PASSW, GMAIL_TO')
        pass


def upload_response_list_to_s3(response_list: list):
    session = boto3.Session(
        aws_access_key_id=os.environ['ACCESS'],
        aws_secret_access_key=os.environ['SECRET']
    )
    s3 = session.resource('s3')
    s3_key = os.environ['S3_KEY']
    s3_bucket = os.environ['S3_BUCKET']
    obj = s3.Object(s3_bucket, s3_key)
    obj.put(Body=json.dumps(response_list))
    print('Uploaded new file to s3')


def lambda_handler(event, context):
    file_from_s3 = read_s3_file()
    list_of_100 = read_last_100(file_from_s3, '2020-05-10')
    analyzed_list_of_100 = analyze_w_last100(list_of_100)
    upload_response_list_to_s3(analyzed_list_of_100)

    return {
        'statusCode': 200,
        'body': json.dumps('Checking costs')
    }

