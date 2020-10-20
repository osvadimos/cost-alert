# AWS cost alert module

The application checks the AWS account expenses and sends a notification in the Slack or email if the threshold exceeded.

>By default, it calculates mean of spends for 100 previous days with daily granularity and sends a notification if on the day of request spends 10%/20% more.

Costs of 01st 02nd days of each month are getting collected and counted separately because of AWS S3 Month Spends Recalculations happens in these days.

Data kept in ```result``` JSON file in your bucket, where each value is a part of a single day request.

### Parameters
Environment variables:
```python
'S3_BUCKET' # – name of your S3 bucket. S3://your-bucket-name/
'S3_KEY' # – path to the result file. /some-folder-in-bucket/result.json
'SLACK_URL' # – your Slack App Incoming Webhook URL
'SLACK_ID' # – Slack personal ID if you want to use a mention
```

In order to deploy you have to complete next steps:
* Install sam cli for aws lambda
https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html
* Build a package with dependencies.
```
cd cost_alert
sam  build && sam package --s3-bucket your-s3-bucket
```
* Upload your lambda onto AWS
* Set up cloudwatch cron alarm and hook it to the current lambda function 
