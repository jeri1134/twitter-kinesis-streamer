import base64
import json
import boto3

print('Loading function')

def lambda_handler(tweets):
    output = []

    for tweet in tweets['records']:
        
        dict_data = base64.b64decode(record['data']).decode('utf-8').strip()
        
        data_record = {
            'message': dict_data,
            'Timestamp': created_at
        }
        print(data_record)
        
        output_record = {
            'tweetId': tweet['tweetId'],
            'result': 'Ok',
            'data': base64.b64encode(json.dumps(data_record).encode('utf-8')).decode('utf-8')
        }
        print(output_record)
        
        output.append(output_record)

    print(output)
    return {'records': output}      