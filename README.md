# AWS cost alert module

This code checks the AWS S3 account spends and sends a notification in the Slack if the threshold is exceeded.

>By default, it calculates mean of spends for 100 previous days with daily granularity and sends a notification if on the day of request spends 10%/20% more.

Spends of 01-02 days of each month are collected and counts separately because of AWS S3 Month Spends Recalculations happens in these days.

Data keeps in ```result``` JSON file in your bucket, where each value is a part of a single request.

### Parameters
Environment variables:
```python
'S3_BUCKET' # – name of your S3 bucket. S3://your-bucket-name/
'S3_KEY' # – path to the result file. /some-folder-in-bucket/result.json
'SLACK_URL' # – your Slack App Incoming Webhook URL
'SLACK_ID' # – Slack personal ID if you want to use a mention
```