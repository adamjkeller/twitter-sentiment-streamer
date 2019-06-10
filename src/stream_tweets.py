#!/usr/bin/env python3

from time import sleep
from os import getenv
from aws import SecretsManager, Comprehend, FireHose, SQSQueue, SSMParameters
import twitter

class TwitterCapture(object):

    def __init__(self, woe_id='23424977'):
        self.woe_id = getenv("WORLD_ID") or woe_id
        self.FIREHOSE_STREAM = getenv("FIREHOSE_NAME") or "NULL"
        self.queue_name = getenv("SQS_QUEUE_NAME") or "NULL"
        self.param_name =  getenv("SSM_PARAM_INITIAL_RUN") or "NULL"
        self.since_date = getenv("SINCE_DATE") or '2019-03-01'
        self.twitter_term = getenv("TWITTER_KEYWORD") or 'maga'
        self.api = self.instantiate_api()

    def instantiate_api(self):
        consumer_key, consumer_secret, access_token, access_token_secret = SecretsManager().setup_secrets()
        return twitter.Api(consumer_key=consumer_key,
                           consumer_secret=consumer_secret,
                           access_token_key=access_token,
                           access_token_secret=access_token_secret,
                           tweet_mode='extended',
                           sleep_on_rate_limit=False)

    def get_trends(self, woe_id):
        return self.api.GetTrendsWoeid(woeid=woe_id)
    
    def search(self, since_date=None, result_type="recent", count=100, include_entities=False, last_item=None, retries=0):
        print("Searching twitter for term \'{}\', since date of \'{}\', and since last tweet id of \'{}\'".format(self.twitter_term, since_date, last_item))
        try:
            return self.api.GetSearch(term=self.twitter_term, result_type=result_type, count=count, include_entities=include_entities, since=since_date, since_id=last_item)
        except Exception as e:
            # Backoff with rate limit
            print(e)
            print("WARNING: backing off, hit rate limit. Counter == {}".format(retries))
            retries = retries + 1
            sleep(2 ** retries)
            self.search(since_date=since_date, last_item=last_item, retries=retries)
        
    def send_to_firehose(self, stream_data):
        print("Shipping data to firehose...")
        response = FireHose().send_to_firehose(firehose_stream_name=self.FIREHOSE_STREAM, stream_data=stream_data)
        print("Shipped! Response: {}".format(response))
        return response

    def cleanup_tweet(self, tweet):
        import re
        return ' '.join(re.sub(r"(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split()) 

    def queue_details(self):
        queue = SQSQueue()
        queue_url = queue.get_queue_url(queue_name=self.queue_name)['QueueUrl']
        return queue, queue_url

    def push_tweet_id_to_queue(self, message):
        queue, queue_url = self.queue_details()
        return queue.put_queue(queue_url=queue_url, message_body=message)

    def get_tweet_id_from_queue(self):
        queue, queue_url = self.queue_details()
        queue_details = queue.get_queue_item(queue_url=queue_url)
        return queue_details['Messages'][0]['Body'], queue_details['Messages'][0]['ReceiptHandle']

    def delete_queue_item(self, receipt_handle):
        queue, queue_url = self.queue_details()
        return queue.del_queue_item(queue_url=queue_url, receipt_id=receipt_handle)

    def clear_visibility_timeout(self, receipt_id):
        # Ensure on exit that we clear out the queued item and proceed
        print("ensuring unprocessed queue item is put back")
        queue, queue_url = self.queue_details()
        queue.set_visibility_timeout(queue_url=queue_url, receipt_id=receipt_id, timeout=0)

    def get_parameter(self):
        return SSMParameters().get_parameter(name=self.param_name)

    def check_if_not_first_run(self):
        # Return False if first run, True if NOT first run
        result = self.get_parameter()['Parameter']['Value']

        if eval(result) is not False:
            return True
        else:
            return False

    def main(self):
        while True:
            self.results_list = []
            last_tweet_id = None
            not_first_run = self.check_if_not_first_run()

            if not_first_run is False:
                print("FIRST RUN, no items in queue yet. Updating parameter to ensure first run is disabled hereafter...")
                SSMParameters().put_parameter(name=self.param_name, value='True')
            else:
                try:
                    last_tweet_id, receipt_id = self.get_tweet_id_from_queue()
                except KeyError:
                    print("Message in queue may be invisible at the moment. Sleeping and will try again shortly...")
                    sleep(10)
                    pass

            _last_tweet = last_tweet_id

            if _last_tweet is not None:
                _search = self.search(last_item=int(_last_tweet))
            else:
                _search = self.search(since_date=self.since_date)

            # Get length of results from search
            try:
                results_count = len(_search)
            except TypeError as t:
                print("No data retured from search, this is a transient error. Sleeping and will try again later.\nERROR: {}".format(t))
                sleep(10)
                pass

            if results_count < 1:
                print("SEARCH RESULTS COUNT: {}. There is nothing to process at this time...".format(results_count))
                self.clear_visibility_timeout(receipt_id=receipt_id)
                sleep(10)
                pass
            else:
                print("SEARCH RESULTS COUNT: {}. Processing the data...".format(results_count))
                # Grabbing most recent tweet from response data
                _last_tweet = _search[0].id_str

                # Sending results to firehose
                [self.send_to_firehose(x._json) for x in _search]

                # Push latest tweet id to queue
                self.push_tweet_id_to_queue(message=_last_tweet)

            # Delete last queue message
            if last_tweet_id is not None and results_count > 0:
                self.delete_queue_item(receipt_handle=receipt_id)


if __name__ == '__main__':
    TwitterCapture().main()