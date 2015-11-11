import sys,json
import tweepy
from tweepy import OAuthHandler

# Twitter API Keys. In general we SHOULD hide these via environment variables
access_token = ''
access_token_secret = ''
consumer_key = ''
consumer_secret = ''

if __name__ == '__main__':

    if len(sys.argv) < 2:
        print "You must specify a query string"
        sys.exit()

    # Connect & Authenticate to Twitter
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth)
    
    query = sys.argv[1]
    max_tweets = 1000
    searched_tweets = [status for status in tweepy.Cursor(api.search, q=query).items(max_tweets)]

    for status in searched_tweets:
        print(json.dumps(status._json))
