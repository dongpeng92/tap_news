# -*- coding: utf-8 -*-

import os
import sys
import datetime
from dateutil import parser

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))
import mongodb_client
from cloudamqp_client import CloudAMQPClient

from sklearn.feature_extraction.text import TfidfVectorizer

NEWS_TABLE_NAME = 'news'

DEDUPE_NEWS_TASK_QUEUE_URL = "amqp://ofmdwnsa:JpyDbMoshiM-rdHgNw2NjoMDnv1A_SDY@fish.rmq.cloudamqp.com/ofmdwnsa"
DEDUPE_NEWS_TASK_QUEUE_NAME = "tap-news-dedupe-news-task-queue"

cloudAMQP_client = CloudAMQPClient(DEDUPE_NEWS_TASK_QUEUE_URL, DEDUPE_NEWS_TASK_QUEUE_NAME)

SLEEP_TIME_IN_SECONDS = 1

SAME_NEWS_SIMILARITY_THRESHOLD = 0.8

def handleMessage(msg):
    if msg is None or not isinstance(msg, dict):
        return
    msg_text = str(msg['text'])
    if msg_text is None:
        return

    # get all recent news based on publishedAt
    published_at = parser.parse(msg['publishedAt'])
    published_at_day_begin = datetime.datetime(published_at.year, published_at.month, published_at.day, 0, 0, 0, 0)
    published_at_day_end = published_at_day_begin + datetime.timedelta(days=1)

    # dedupe from mongodb
    db = mongodb_client.get_db()
    recent_news_list = list(db[NEWS_TABLE_NAME].find({'publishedAt': {'$gte': published_at_day_begin, '$lt': published_at_day_end}}))

    if recent_news_list is not None and len(recent_news_list) > 0:
        documents = [str(news['text']) for news in recent_news_list]
        documents.insert(0, msg_text)

        # calculate TD-IDF matrix
        tfidf = TfidfVectorizer().fit_transform(documents)
        pairwise_sim = tfidf * tfidf.T

        print pairwise_sim.A

        rows, _ = pairwise_sim.shape

        # compare with recent news
        for row in range(1, rows):
            if pairwise_sim[row, 0] > SAME_NEWS_SIMILARITY_THRESHOLD:
                print "Duplicated news. Ignore."
                return

    msg['publishedAt'] = parser.parse(msg['publishedAt'])
    # put into mongodb
    db[NEWS_TABLE_NAME].replace_one({'digest': msg['digest']}, msg, upsert=True)

while True:
    if cloudAMQP_client is not None:
        msg = cloudAMQP_client.getMessage()
        if msg is not None:
            try:
                handleMessage(msg)
            except Exception as e:
                print e
                pass
        cloudAMQP_client.sleep(SLEEP_TIME_IN_SECONDS)