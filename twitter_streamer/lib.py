from datetime import datetime
import json

def create_timestamped_filename():

    credentials = get_credentials()
    username = credentials['username']
    timestamp_str = str(datetime.now())
    timestamp_str = (timestamp_str.replace(' ', '_')
                                  .replace('.', '-')
                                  .replace(':', '-'))
    filename = "tweets-" + username + '-' + timestamp_str + ".json"
    return filename

def get_credentials():
    with open("credentials.json", 'r') as stream:
        credentials = json.load(stream)
    return credentials

def timestamp():
    now = datetime.now().strftime('%D %H:%M:%S')
    print(now, end=' | ')
    print('Collecting Tweets', end=' | ')

def write_to_disk(tweets):
    filename = create_timestamped_filename()
    with open(filename, 'w') as outfile:
        json.dump(tweets, outfile)
    print('Written to Disk', end=' | ')
    return filename


import boto3
import json
from utility import create_timestamped_filename, get_credentials
from os import remove

def create_boto_client():
    s3 = boto3.resource('s3')

    credentials = get_credentials()

    client = boto3.client('s3',
                          aws_access_key_id=credentials['aws']['aws_access_key_id'],
                          aws_secret_access_key=credentials['aws']['aws_secret_access_key'])
    print("Created S3 Client")
    return client

def list_files_in_S3_bucket(client):

    credentials = get_credentials()
    s3_bucket = credentials['s3_bucket']

    objects = client.list_objects(Bucket=s3_bucket)
    objects_df = DataFrame(objects['Contents'])
    return list(objects_df.Key.values)

def process_local_file_to_S3(client, filename):
        write_file_to_S3(client, filename)
        remove(filename)
        print('Written to S3', end=' | ')

def read_object_from_S3(client, key):

    credentials = get_credentials()
    s3_bucket = credentials['s3_bucket']
    object_reference = client.get_object(Key=key,
                                         Bucket=s3_bucket)
    object_body = object_reference['Body']
    tweet_data = json.loads(object_body.read().decode())
    return tweet_data

def write_file_to_S3(client, filename):
    credentials = get_credentials()
    s3_bucket = credentials['s3_bucket']
    with open(filename) as infile:
        json_data=infile.read()
        client.put_object(Key=filename,
                          Body=json_data,
                          Bucket=s3_bucket)

from pymongo import MongoClient
from s3 import read_object_from_S3
from utility import get_credentials

def create_mongo_client_to_database_collection():
    credentials = get_credentials()

    client = MongoClient(credentials['mongo']['ip'],
                         credentials['mongo']['port'])
    database = client.get_database(credentials['mongo']['database'])
    collection = database.get_collection(credentials['mongo']['collection'])
    print("Created Mongo Client")
    return collection

def insert_to_mongo(s3_client, collection_client, key):
    tweets_from_s3 = read_object_from_S3(s3_client, key)
    collection_client.insert_many(tweets_from_s3)
    print('Inserted to Mongo')

from datetime import datetime
import json
from os import rename

from s3 import create_boto_client, process_local_file_to_S3
from twitter_funcs import collect_tweets, create_tweet_iterator
from mongo import create_mongo_client_to_database_collection, insert_to_mongo
from utility import get_credentials, timestamp, write_to_disk
from requests import HTTPError

if __name__ == "__main__":

    credentials = get_credentials()
    if credentials['twitter']['token'] is None:
        print("Did you forget to add your twitter tokens to the credentials.json file?")
        raise HTTPError

    tweet_iterator    = create_tweet_iterator()
    s3_client         = create_boto_client()
    collection_client = create_mongo_client_to_database_collection()

    while True:
        timestamp()
        tweets   = collect_tweets(tweet_iterator, 100)
        filename = write_to_disk(tweets)
        process_local_file_to_S3(s3_client,filename)
        insert_to_mongo(s3_client, collection_client, filename)