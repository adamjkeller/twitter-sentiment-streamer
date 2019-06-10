#!/usr/bin/env python3

from time import sleep, strftime
from os import getenv
from botocore.exceptions import ClientError
from aws import Comprehend, FireHose, S3
from json import JSONDecodeError
import json
import boto3

class SentimentAnalysis(object):

    def __init__(self):
        self.FIREHOSE_STREAM = getenv("FIREHOSE_STREAM") or "NULL"

    def get_sentiment(self, tweet):
        response = Comprehend().sentiment(self.cleanup_tweet(tweet))
        try:
            sentiment = response['Sentiment']
            sentiment_score = response['SentimentScore']
            return sentiment, sentiment_score
        except Exception as e:
            print("ERROR: {}".format(e))
            return None, None

    def send_to_firehose(self, stream_data):
        response = FireHose().send_to_firehose(firehose_stream_name=self.FIREHOSE_STREAM, stream_data=stream_data)
        return response

    def cleanup_tweet(self, tweet):
        import re
        return ' '.join(re.sub(r"(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split()) 

    def convert_datestamp(self, to_convert):
        import time
        _date_updated = time.strptime(to_convert, '%a %b %d %H:%M:%S %z %Y') 
        return time.strftime('%d/%m/%Y %H:%M:%S', _date_updated)

    def firehose(self, raw_tweet_data):
        if raw_tweet_data.get('retweeted_status'):
            created_date = raw_tweet_data.get('created_at')
            tweet = raw_tweet_data['retweeted_status']['full_text']
            tweet_id = raw_tweet_data['retweeted_status']['id']
            tweet_length = len(tweet)
        else:
            created_date = raw_tweet_data.get('created_at')
            tweet = raw_tweet_data['full_text']
            tweet_id = raw_tweet_data['id']
            tweet_length = len(tweet)

        sentiment, sentiment_details = self.get_sentiment(tweet)
        converted_timestamp = self.convert_datestamp(created_date)

        stream_data = {
            "time_stamp": converted_timestamp,
            "tweet": tweet,
            "tweet_id": tweet_id,
            "sentiment": sentiment,
            "sentiment_details": sentiment_details
        }

        # Ship data to firehose which will put in curated s3 bucket
        if sentiment is not None:
            self.send_to_firehose(stream_data=stream_data)
        else:
            print("ERROR: Unable to record sentiment. Stream data details: {}".format(stream_data))

    def main(self, body):
        decoder = json.JSONDecoder()
        decode_index = 0
        tweet_data, _ = decoder.raw_decode(body, decode_index)
        content_length = len(body)
        while decode_index < content_length:
            try:
                tweet_data, decode_index = decoder.raw_decode(body, decode_index)
                print("File index:", decode_index)
                self.firehose(raw_tweet_data=tweet_data)
            except JSONDecodeError as e:
                print("JSONDecodeError:", e)
                # Scan forward and keep trying to decode
                decode_index += 1


def lambda_handler(event, context):
    for _event in event['Records']:
        data = S3().read_object(_event['s3']['bucket']['name'], _event['s3']['object']['key'])
        SentimentAnalysis().main(body=data)


if __name__ == '__main__':
    example_data = {'created_at': 'Wed Jun 05 22:26:29 +0000 2019', 'id': 1136398915065057280, 'id_str': '1136398915065057280', 'full_text': 'RT @LouDobbs: Join Lou tonight – Radical Dimms, RINOS, Chamber of Horrors subverting @RealDonaldTrump’s Mexico tariffs &amp; selling out our co…', 'truncated': False, 'display_text_range': [0, 144], 'entities': {'hashtags': [], 'symbols': [], 'user_mentions': [{'screen_name': 'LouDobbs', 'name': 'Lou Dobbs', 'id': 26487169, 'id_str': '26487169', 'indices': [3, 12]}, {'screen_name': 'realDonaldTrump', 'name': 'Donald J. Trump', 'id': 25073877, 'id_str': '25073877', 'indices': [85, 101]}], 'urls': []}, 'metadata': {'iso_language_code': 'en', 'result_type': 'recent'}, 'source': '<a href="http://twitter.com/download/android" rel="nofollow">Twitter for Android</a>', 'in_reply_to_status_id': None, 'in_reply_to_status_id_str': None, 'in_reply_to_user_id': None, 'in_reply_to_user_id_str': None, 'in_reply_to_screen_name': None, 'user': {'id': 3421327821, 'id_str': '3421327821', 'name': 'Tommy Byrnes1', 'screen_name': 'TommyByrnes1', 'location': '', 'description': 'I stand with President Trump & will vote for him in 2020. Build the wall now. Support Judicial Watch. MAGA.\n NRA member. Watch Lou Dobbs, #ditchmitch', 'url': None, 'entities': {'description': {'urls': []}}, 'protected': False, 'followers_count': 1067, 'friends_count': 1192, 'listed_count': 28, 'created_at': 'Fri Aug 14 01:14:17 +0000 2015', 'favourites_count': 59143, 'utc_offset': None, 'time_zone': None, 'geo_enabled': False, 'verified': False, 'statuses_count': 44292, 'lang': 'en', 'contributors_enabled': False, 'is_translator': False, 'is_translation_enabled': False, 'profile_background_color': 'C0DEED', 'profile_background_image_url': 'http://abs.twimg.com/images/themes/theme1/bg.png', 'profile_background_image_url_https': 'https://abs.twimg.com/images/themes/theme1/bg.png', 'profile_background_tile': False, 'profile_image_url': 'http://pbs.twimg.com/profile_images/970057429131120640/E7p2-sMY_normal.jpg', 'profile_image_url_https': 'https://pbs.twimg.com/profile_images/970057429131120640/E7p2-sMY_normal.jpg', 'profile_banner_url': 'https://pbs.twimg.com/profile_banners/3421327821/1496364424', 'profile_link_color': '1DA1F2', 'profile_sidebar_border_color': 'C0DEED', 'profile_sidebar_fill_color': 'DDEEF6', 'profile_text_color': '333333', 'profile_use_background_image': True, 'has_extended_profile': False, 'default_profile': True, 'default_profile_image': False, 'following': False, 'follow_request_sent': False, 'notifications': False, 'translator_type': 'none'}, 'geo': None, 'coordinates': None, 'place': None, 'contributors': None, 'retweeted_status': {'created_at': 'Wed Jun 05 21:48:06 +0000 2019', 'id': 1136389256392450049, 'id_str': '1136389256392450049', 'full_text': 'Join Lou tonight – Radical Dimms, RINOS, Chamber of Horrors subverting @RealDonaldTrump’s Mexico tariffs &amp; selling out our country’s safety &amp; security. Join Lou at 7PM ET. #MAGA #AmericaFirst #Dobbs', 'truncated': False, 'display_text_range': [0, 206], 'entities': {'hashtags': [{'text': 'MAGA', 'indices': [180, 185]}, {'text': 'AmericaFirst', 'indices': [186, 199]}, {'text': 'Dobbs', 'indices': [200, 206]}], 'symbols': [], 'user_mentions': [{'screen_name': 'realDonaldTrump', 'name': 'Donald J. Trump', 'id': 25073877, 'id_str': '25073877', 'indices': [71, 87]}], 'urls': []}, 'metadata': {'iso_language_code': 'en', 'result_type': 'recent'}, 'source': '<a href="http://twitter.com" rel="nofollow">Twitter Web Client</a>', 'in_reply_to_status_id': None, 'in_reply_to_status_id_str': None, 'in_reply_to_user_id': None, 'in_reply_to_user_id_str': None, 'in_reply_to_screen_name': None, 'user': {'id': 26487169, 'id_str': '26487169', 'name': 'Lou Dobbs', 'screen_name': 'LouDobbs', 'location': 'New York, NY', 'description': 'Lou Dobbs Tonight, Fox Business Network, 7 & 10 pm IG: https://t.co/Mqnxd3lgtA', 'url': 'https://t.co/mRPE2ZuJkU', 'entities': {'url': {'urls': [{'url': 'https://t.co/mRPE2ZuJkU', 'expanded_url': 'http://loudobbs.com', 'display_url': 'loudobbs.com', 'indices': [0, 23]}]}, 'description': {'urls': [{'url': 'https://t.co/Mqnxd3lgtA', 'expanded_url': 'http://Instagram.com/loudobbstonight/', 'display_url': 'Instagram.com/loudobbstonigh…', 'indices': [55, 78]}]}}, 'protected': False, 'followers_count': 1944892, 'friends_count': 2081, 'listed_count': 5162, 'created_at': 'Wed Mar 25 12:39:59 +0000 2009', 'favourites_count': 13620, 'utc_offset': None, 'time_zone': None, 'geo_enabled': True, 'verified': True, 'statuses_count': 31992, 'lang': 'en', 'contributors_enabled': False, 'is_translator': False, 'is_translation_enabled': False, 'profile_background_color': 'C0DEED', 'profile_background_image_url': 'http://abs.twimg.com/images/themes/theme1/bg.png', 'profile_background_image_url_https': 'https://abs.twimg.com/images/themes/theme1/bg.png', 'profile_background_tile': False, 'profile_image_url': 'http://pbs.twimg.com/profile_images/663851941571641344/OqqkE56l_normal.jpg', 'profile_image_url_https': 'https://pbs.twimg.com/profile_images/663851941571641344/OqqkE56l_normal.jpg', 'profile_banner_url': 'https://pbs.twimg.com/profile_banners/26487169/1555706370', 'profile_link_color': '0084B4', 'profile_sidebar_border_color': '000000', 'profile_sidebar_fill_color': 'DDEEF6', 'profile_text_color': '333333', 'profile_use_background_image': True, 'has_extended_profile': False, 'default_profile': False, 'default_profile_image': False, 'following': False, 'follow_request_sent': False, 'notifications': False, 'translator_type': 'none'}, 'geo': None, 'coordinates': None, 'place': None, 'contributors': None, 'is_quote_status': False, 'retweet_count': 141, 'favorite_count': 347, 'favorited': False, 'retweeted': False, 'lang': 'en'}, 'is_quote_status': False, 'retweet_count': 141, 'favorite_count': 0, 'favorited': False, 'retweeted': False, 'lang': 'en'}
    SentimentAnalysis().firehose(example_data)