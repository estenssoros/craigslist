import boto
import boto.sns
from boto.s3.connection import OrdinaryCallingFormat
import os


def aws_keys():
    '''
    INPUT: None
    OUTPUT: aws access_key and secret_access_key
    '''
    env = os.environ
    access_key = env['AWS_ACCESS_KEY_ID']
    access_secret_key = env['AWS_SECRET_ACCESS_KEY']
    return access_key, access_secret_key


def connect_s3():

    access_key, access_secret_key = aws_keys()

    conn = boto.connect_s3(access_key, access_secret_key, calling_format=OrdinaryCallingFormat())
    bucket_name = 'estenssoros.com'
    for bucket in conn.get_all_buckets():
        if bucket.name == bucket_name:
            break
    return bucket

def send_message(message):
    region = [r for r in boto.sns.regions() if r.name == 'us-east-1'][0]
    access_key, access_secret_key = aws_keys()
    sns = boto.sns.SNSConnection(aws_access_key_id=access_key,
                                 aws_secret_access_key=access_secret_key,
                                 region=region)
    sns.publish(topic="arn:aws:sns:us-east-1:001928331621:notifications",
            message=message)
    print 'message sent!'

if __name__ == '__main__':
    b = connect_s3()
