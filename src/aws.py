#!/usr/bin/env python

import boto3
from botocore.exceptions import ClientError, ParamValidationError
from time import sleep
from json import dumps


class SecretsManager(object):
    
    def __init__(self):
        self.client = boto3.client('secretsmanager') 

    def setup_secrets(self, secret_id='prod/twitter-secrets'):
        """
        Expecting a dictionary that will return the necessary secrets to access Twitter API
        """
        secrets = self.client.get_secret_value(
            SecretId=secret_id
        )

        secrets = eval(secrets['SecretString'])
        return secrets['consumer_key'], secrets['consumer_secret'], secrets['access_token'], secrets['access_token_secret']


class Comprehend(object):
    def __init__(self):
        self.client = boto3.client('comprehend')

    def sentiment(self, tweet, retries=0):
        try:
            result = self.client.detect_sentiment(
                Text=tweet,
                LanguageCode='en',
            )
            return result
        except ClientError as e:
            if e.response['Error']['Code'] == 'ThrottlingException':
                print("WARNING: Throttled Exception, backing off...")
                retries = retries + 1
                sleep(2 ** retries)
                return self.sentiment(tweet=tweet, retries=retries)
            else:
                print(e.response)
        except ParamValidationError as pe:
            print("ERROR: Something went wrong, received \"ParamValidationError\": {}. Tweet info: {}".format(pe, tweet))
            pass


class FireHose(object):
    def __init__(self):
        self.client = boto3.client('firehose')

    def send_to_firehose(self, firehose_stream_name, stream_data):
        # Ship json record to firehose stream
        return self.client.put_record(
            DeliveryStreamName=firehose_stream_name,
            Record={
                'Data': dumps(stream_data)
            }
        )


class SQSQueue(object):
    def __init__(self):
        self.client = boto3.client('sqs')

    def get_queue_url(self, queue_name):
        return self.client.get_queue_url(
            QueueName=queue_name
        )

    def put_queue(self, message_body, queue_url):
        return self.client.send_message(
            QueueUrl=queue_url,
            MessageBody=message_body,
            MessageGroupId=message_body,
        )

    def get_queue_item(self, queue_url):
        return self.client.receive_message(
            QueueUrl=queue_url,
        )

    def del_queue_item(self, queue_url, receipt_id):
        return self.client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_id
        )

    def set_visibility_timeout(self, queue_url, receipt_id, timeout=0):
        return self.client.change_message_visibility(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_id,
            VisibilityTimeout=timeout
        )

        
class SSMParameters(object):
    def __init__(self):
        self.client = boto3.client('ssm')

    def get_parameter(self, name):
        return self.client.get_parameter(
            Name=name
        )

    def put_parameter(self, name, value):
        return self.client.put_parameter(
            Name=name,
            Value=value,
            Type='String',
            Overwrite=True
        )

class S3(object):
    def __init__(self):
        self.resource = boto3.resource('s3')

    def read_object(self, bucket_name, bucket_key):
        obj = self.resource.Object(bucket_name, bucket_key)
        return obj.get()['Body'].read().decode('utf-8')
