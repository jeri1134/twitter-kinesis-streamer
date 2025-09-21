import sys
import boto3
import time
import logging
import argparse
import requests

sys.path.append("./ListenerScript/")

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class  KinesisStreamer:
    def __init__(self, region='us_east_1'):
        self.kinesis_client = boto3.client('kinesis', region_name=region)
    
    def send_record(self, stream_name, data, partition_key="No"):
        try:
            response = self.kinesis_client.put_record(
                StreamName=stream_name,
                Data=data,
                PartitionKey=partition_key
            )
        except self.kinesis_client.exceptions.ResourceNotFoundException:
            logger.error(f"Kinesis Stream: '{stream_name}' not found")
            sys.exit(1)


def AutomateDataStreamInput():

    records = []
    
    for tweet in tweets:
        records.append(tweet)
    
    print("aws kinesis put-records \
          --stream-name somestram\
          --data somedata\
          --partition-key pk")