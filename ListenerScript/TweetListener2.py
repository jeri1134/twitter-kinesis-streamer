import tweepy
import time
import logging
import argparse
import sys
import boto3

'''
api_key = "hgmN6Jn1OznANq9jpN0ws4UEM"
api_secret = "tU9OGaAAH79Td6mNs57NehY5YCjBUptlJ9cyhcnyZ4M7FC6uVt"
access_token = "1969273945024274432-0iP79JKYkhszvODdyI9iUL2AiYH9SV"
access_token_secret = "mbPt9HDs8cg9gsUqtqGJdzX1kFIlVUT28GWPrUjyz9iy1"
'''
bearer = "AAAAAAAAAAAAAAAAAAAAAI8m4QEAAAAARRDOaTXBCyNZ0izNXzoOzC1%2FB3w%3DnqKMqMGWRrmQJSN9B9juiVYNk1tYzWSZzwm75nC3BzMymqXbX6"

# Earthquake, Landslide, Flood, Wildfire 
# Filter tweets for it to be original and no retweets or replies
#"'Banjir' 'Kelantan' -filter:retweets AND -filter:replies AND -filter:links"
q = "(#landslide OR #tanahruntuh OR #flood) (#malaysia OR #terengganu OR #sabah) -is:retweet -is:reply"
num_of_tweets = 10

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KinesisStreamer:
    def __init__(self, region_name='eu-central-1'):
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
    """
    Defines the command-line arguments 
    """
    parser = argparse.ArgumentParser(description="Send CSV data to Kinesis Data Streams")
    parser.add_argument("--stream_name", "-sn", required=True, help="Name of the Kinesis Data Stream")
    parser.add_argument("--interval", "-i", type=int, required=True, help="Time interval (in seconds) between two writes")
    args = parser.parse_args()

    return args

def send_twitter_api_to_kinesis(stream_name, interval):
    kinesis_streamer = KinesisStreamer()


# Initiate tweepy API
client = tweepy.Client(bearer_token=bearer)

def search_tweets_v1():
    try:
        # Requirement of the request query
        tweets = client.search_recent_tweets(
            query = q, 
            max_results = num_of_tweets,
            tweet_fields = ["created_at", "author_id"]  # Request tweet metadata
        )
        
        # Will check if there's any tweets with the requirements 
        if not tweets.data:
            print("No tweets found")
        
        # Create lists tweets
        tweet_texts = []
        i = 1
        # Extract data from tweets
        for tweet in tweets.data:
            
            tweet_info = {
                "Number": i,
                "Tweet": str(tweet.text),
                "Timestamp": str(tweet.created_at)
            }
            tweet_texts.append(tweet_info)
            print(tweet_info)
            print(tweet_texts)
            i += 1

    except tweepy.TooManyRequests as e:
        print('Please wait for 15 min, Rate has hit the limit')
        print(f"Error details: {str(e)}")
        time.sleep(900)
        
    except tweepy.Forbidden as e:
        print(f'Access forbidden - check your API access level: {str(e)}')
        
    except AttributeError as e:
        print(f'Attribute error - missing field in API response: {str(e)}')
        print("Make sure you're requesting the right fields from the API")
        
    except Exception as e:
        print(f'Error due to {str(e)}')
        print(f'Error type: {type(e).__name__}')

# Run the function
print("Fetching Twitter API...")
df = search_tweets_v1()

if __name__ == "__main__":
    args = define_arguments()

    logger.info(f"Sending Twitter API To Kinesis...")

