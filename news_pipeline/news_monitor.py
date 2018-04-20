# -*- coding: utf-8 -*-

import os
import sys
import redis
import hashlib
import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))

import news_api_client
from cloudamqp_client import CloudAMQPClient

REDIS_HOST = 'localhost'
REDIS_PORT= 6379

NEWS_TIME_OUT_IN_SECONDS = 3600 * 24
SLEEP_TIME_IN_SECONDS = 5 * 60

SCRAPE_NEWS_TASK_QUEUE_URL = "amqp://ofmdwnsa:JpyDbMoshiM-rdHgNw2NjoMDnv1A_SDY@fish.rmq.cloudamqp.com/ofmdwnsa"
SCRAPE_NEWS_TEST_QUEUE_NAME = 'tap-news-scrape-news-task-queue'

NEWS_SOURCES = [
    'bbc-news',
    'bbc-sport',
    'bloomberg',
    'cnn',
    'entertainment-weekly',
    'espn',
    'ign',
    'techcrunch',
    'the-new-york-times',
    'the-wall-street-journal',
    'the-washington-post'
]

redis_client = redis.StrictRedis(REDIS_HOST, REDIS_PORT)
cloudAMQP_client = CloudAMQPClient(SCRAPE_NEWS_TASK_QUEUE_URL, SCRAPE_NEWS_TEST_QUEUE_NAME)

while True:
    news_list = news_api_client.getNewsFromSource(NEWS_SOURCES)
    nums_of_new_news = 0

    for news in news_list:
        news_digest = hashlib.md5(news['title'].encode('utf-8')).digest().encode('base64')

        # compare with redis if the news already exist
        if redis_client.get(news_digest) is None:
            nums_of_new_news = nums_of_new_news + 1
            news['digest'] = news_digest

            # give value of publishedAt
            if news['publishedAt'] is None:
                news['publishedAt'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

            redis_client.set(news_digest, news)
            redis_client.expire(news_digest, NEWS_TIME_OUT_IN_SECONDS)

            cloudAMQP_client.sendMessage(news)

    print "Fetched %d new news." % nums_of_new_news
    cloudAMQP_client.sleep(SLEEP_TIME_IN_SECONDS)