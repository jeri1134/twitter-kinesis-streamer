import json
import boto3
import tweepy
import os

def lambda_handler(event, context):
    # Initialize with minimal resources
    sqs = boto3.client('sqs')
    s3 = boto3.client('s3')
    
    # Collect only essential data
    tweets = collect_disaster_tweets(max_count=50)  # Limit API calls
    weather_data = get_weather_summary()  # Use free APIs only
    
    # Batch process to reduce costs
    batch_data = {
        'timestamp': context.aws_request_id,
        'tweets': tweets,
        'weather': weather_data
    }
    
    # Send to SQS for processing (cheaper than Kinesis)
    sqs.send_message(
        QueueUrl=os.environ['SQS_QUEUE_URL'],
        MessageBody=json.dumps(batch_data)
    )
    
    # Store raw data in S3 (cheaper than processing immediately)
    s3.put_object(
        Bucket=os.environ['S3_BUCKET'],
        Key=f"raw/{context.aws_request_id}.json",
        Body=json.dumps(batch_data)
    )
    
    return {'statusCode': 200}

def collect_disaster_tweets(max_count=10):
    # Use free Twitter API limits efficiently
    query = "banjir OR landslide malaysia -is:retweet"
    # Process efficiently to stay within rate limits
    pass

def get_weather_summary():
    # Use OpenWeatherMap free tier (1000 calls/day)
    # Focus on Malaysian regions only
    pass