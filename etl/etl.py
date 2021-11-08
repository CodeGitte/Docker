import time
import pymongo
from sqlalchemy import create_engine
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import requests
import logging

#instantiates sentiment analyzer
analyser = SentimentIntensityAnalyzer()

#connects to mongo
#0.0.0.0:27017
#(host='mongodb', port=27017)
client = pymongo.MongoClient(host='mongodb', port=27017)
db = client.twitter

#time
time.sleep(10)

#connects to postgres
#postgresdb:5432
#0.0.0.0:5555
pg = create_engine('postgresql://test:1234@postgresdb:5432/postgres_db', echo=True)

#create postgres table
pg.execute('''
    CREATE TABLE IF NOT EXISTS tweets (
    user_name VARCHAR(500),
    text VARCHAR(500),
    sentiment VARCHAR(500)
);
''')

#extracts tweets from mongodb
def extract():
    ''' Extracts tweets from mongodb'''
    extracted_tweets = list(db.tweets.find())
    return extracted_tweets

#transforms tweets
def transform(extracted_tweets):
    ''' Transforms data: clean text, gets sentiment analysis from text, formats date '''
    transformed_tweets = []
    for tweet in extracted_tweets:
        sentiment = analyser.polarity_scores(tweet['text']) 
        tweet['sentiment'] = sentiment['compound']
        transformed_tweets.append(tweet) 
    return transformed_tweets

#loads the tweets into postgres
def load(transformed_tweets):
    ''' Load final data into postgres'''
    for tweet in transformed_tweets:
        insert_query = "INSERT INTO tweets VALUES (%s, %s, %s);"
        pg.execute(insert_query, (tweet['username'], tweet['text'], tweet['sentiment']))

#executes when running
if __name__== '__main__':
    while True:
        extracted_tweets = extract()
        transformed_tweets = transform(extracted_tweets)
        load(transformed_tweets)
