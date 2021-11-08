from pymongo import database
import tweepy
import credentials
import pymongo
import logging

#connecting to mongo database
client = pymongo.MongoClient(host="mongodb", port=27017)
db = client.twitter

#defining tweets
christmas_hits= ['lastchristmas', 'wham', 'alliwantforchristmas', 'mariahcarey']

#authentication to the twitter api 
def get_auth_handler():
    auth = tweepy.OAuthHandler(credentials.API_KEY, credentials.API_SECRET)
    auth.set_access_token(credentials.ACCESS_TOKEN, credentials.ACCESS_TOKEN_SECRET)
    return auth

class MaxTweetsListener(tweepy.StreamListener):
    def __init__(self, max_tweets, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_tweets = max_tweets
        self.counter = 0

    def on_status(self, status):
        tweet = {
            'text': status.text,
            'username': status.user.screen_name,
        }
        db.tweets.insert_one(tweet)

    def on_error(self, status):
        if status == 420:
            return False

if __name__ == '__main__':
    auth = get_auth_handler()
    listener = MaxTweetsListener(max_tweets=100)
    stream = tweepy.Stream(auth, listener)
    stream.filter(track=christmas_hits, is_async=False)