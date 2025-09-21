#!/usr/bin/env python3

import tweepy
import time
import logging
import argparse
import sys
import boto3
import json
from datetime import datetime

# Twitter Bearer Token - replace with your actual token
bearer = "AAAAAAAAAAAAAAAAAAAAAI8m4QEAAAAARRDOaTXBCyNZ0izNXzoOzC1%2FB3w%3DnqKMqMGWRrmQJSN9B9juiVYNk1tYzWSZzwm75nC3BzMymqXbX6"

# Search query and configuration
q = "(#landslide OR #tanahruntuh OR #flood OR #banjir) (#malaysia OR #terengganu OR #sabah) -is:retweet -is:reply"
num_of_tweets = 10

# Setup logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KinesisStreamer:
    def __init__(self, region_name='us-east-1'):
        self.kinesis_client = boto3.client('kinesis', region_name=region_name)

    def send_record(self, stream_name, data, partition_key="No"):
        try:
            response = self.kinesis_client.put_record(
                StreamName=stream_name,
                Data=data,
                PartitionKey=partition_key
            )
            return response['SequenceNumber']
        except self.kinesis_client.exceptions.ResourceNotFoundException:
            logger.error(f"Kinesis stream '{stream_name}' not found. Please ensure the stream exists.")
            sys.exit(1)

def define_arguments():
    """Defines the command-line arguments"""
    parser = argparse.ArgumentParser(description="Send Twitter data to Kinesis Data Streams")
    parser.add_argument("--stream_name", "-sn", required=True, help="Name of the Kinesis Data Stream")
    parser.add_argument("--interval", "-i", type=int, required=True, help="Time interval (in seconds) between two writes")
    args = parser.parse_args()
    return args

def search_tweets():
    """Search for tweets and return tweet data"""
    client = tweepy.Client(bearer_token=bearer)
    
    try:
        logger.info(f"Searching tweets with query: {q}")
        tweets = client.search_recent_tweets(
            query=q, 
            max_results=num_of_tweets,
            tweet_fields=["created_at", "author_id"]
        )
        
        if not tweets.data:
            logger.warning("No tweets found")
            return []
        
        tweet_list = []
        for i, tweet in enumerate(tweets.data, 1):
            tweet_info = {
                "number": i,
                "tweet_id": str(tweet.id),
                "tweet_text": tweet.text,
                "author_id": str(tweet.author_id),
                "created_at": tweet.created_at.isoformat() if tweet.created_at else None,
                "processed_at": datetime.utcnow().isoformat()
            }
            tweet_list.append(tweet_info)
            logger.info(f"Tweet {i}: {tweet.text[:100]}...")
        
        logger.info(f"Retrieved {len(tweet_list)} tweets")
        return tweet_list
        
    except tweepy.TooManyRequests as e:
        logger.error('Rate limit hit, waiting 15 minutes')
        time.sleep(900)
        return []
        
    except tweepy.Forbidden as e:
        logger.error(f'Access forbidden: {str(e)}')
        return []
        
    except Exception as e:
        logger.error(f'Error: {str(e)}')
        return []

def send_twitter_api_to_kinesis(stream_name, interval):
    """Main function to fetch Twitter data and send to Kinesis"""
    kinesis_streamer = KinesisStreamer()
    
    logger.info(f"Starting Twitter to Kinesis streaming...")
    logger.info(f"Stream: {stream_name}, Interval: {interval} seconds")
    
    batch_number = 1
    
    try:
        while True:
            logger.info(f"Processing batch {batch_number}")
            
            # Get tweets
            tweets = search_tweets()
            
            # Prepare payload
            payload = {
                "batch_number": batch_number,
                "timestamp": datetime.utcnow().isoformat(),
                "tweet_count": len(tweets),
                "query": q,
                "tweets": tweets
            }
            
            # Convert to JSON
            json_data = json.dumps(payload, ensure_ascii=False)
            
            # Send to Kinesis
            try:
                partition_key = f"batch_{batch_number}"
                sequence_number = kinesis_streamer.send_record(
                    stream_name=stream_name,
                    data=json_data,
                    partition_key=partition_key
                )
                logger.info(f"Sent batch {batch_number} to Kinesis, sequence: {sequence_number}")
                
            except Exception as e:
                logger.error(f"Failed to send to Kinesis: {str(e)}")
            
            batch_number += 1
            
            # Wait for next interval
            logger.info(f"Waiting {interval} seconds...")
            time.sleep(interval)
            
    except KeyboardInterrupt:
        logger.info("Stopping service...")
    except Exception as e:
        logger.error(f"Error in main loop: {str(e)}")

if __name__ == "__main__":
    args = define_arguments()
    logger.info("Starting Twitter API to Kinesis service")
    send_twitter_api_to_kinesis(args.stream_name, args.interval)