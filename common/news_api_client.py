import requests
from json import loads

NEWS_API_ENDPOINT = 'https://newsapi.org/v2/'
NEWS_API_KEY = '0ee73aca6d9b456686256b0ebc9794b4'
ARTICLES_TYPE = 'top-headlines'

CNN = 'cnn'
DEFAULT_SOURCES = [CNN]

def buildUrl(end_point=NEWS_API_ENDPOINT, api_name=ARTICLES_TYPE):
    return end_point + api_name

def getNewsFromSource(sources=DEFAULT_SOURCES):
    articles = []

    # get news from news api
    for source in sources:
        payload = {'apiKey': NEWS_API_KEY,
                   'sources': source,
                   'pageSize': 100}
        response = requests.get(buildUrl(), params=payload)
        res_json = loads(response.content)

        # extract info from response
        if (res_json is not None and
            res_json['status'] == 'ok'):

            articles.extend(res_json['articles'])

    return articles
