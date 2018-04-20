# -*- coding: utf-8 -*-

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))

from cloudamqp_client import CloudAMQPClient

from newspaper import Article

# use AMQP queue
DEDUPE_NEWS_TASK_QUEUE_URL = "amqp://ofmdwnsa:JpyDbMoshiM-rdHgNw2NjoMDnv1A_SDY@fish.rmq.cloudamqp.com/ofmdwnsa"
DEDUPE_NEWS_TASK_QUEUE_NAME = "tap-news-dedupe-news-task-queue"
SCRAPE_NEWS_TASK_QUEUE_URL = "amqp://ofmdwnsa:JpyDbMoshiM-rdHgNw2NjoMDnv1A_SDY@fish.rmq.cloudamqp.com/ofmdwnsa"
SCRAPE_NEWS_TASK_QUEUE_NAME = "tap-news-scrape-news-task-queue"

SLEEP_TIME_IN_SECONDS = 5

dedupe_news_queue_client = CloudAMQPClient(DEDUPE_NEWS_TASK_QUEUE_URL, DEDUPE_NEWS_TASK_QUEUE_NAME)
scrape_news_queue_client = CloudAMQPClient(SCRAPE_NEWS_TASK_QUEUE_URL, SCRAPE_NEWS_TASK_QUEUE_NAME)

def handleMessage(msg):
    if msg is None or not isinstance(msg, dict):
        print "Message is broken."
        return

    # give url to newspaper to get article
    article = Article(msg['url'])
    article.download()
    article.parse()

    print article.text
    msg['text'] = article.text
    dedupe_news_queue_client.sendMessage(msg)

while True:
    # fetch message from queue
    if scrape_news_queue_client is not None:
        msg = scrape_news_queue_client.getMessage()
        if msg is not None:
            # handle message to get the articles
            try:
                handleMessage(msg)
            except Exception as e:
                print e
                pass
            scrape_news_queue_client.sleep(SLEEP_TIME_IN_SECONDS)