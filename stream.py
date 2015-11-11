from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

# Twitter API Keys
access_token = ''
access_token_secret = ''
consumer_key = ''
consumer_secret = ''

# Basic listener. Prints tweets to stdout
class stdoutListener(StreamListener):
    def on_data(self, data):
        print data
        return True
    def on_error(self,status):
        print(status)

if __name__ == '__main__':
    # Connect & Authenticate to Twitter
    listener = stdoutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, listener)
    
    stream.filter(track=['Clinton', 'Sanders', 'Trump', 'Carson', 'Rubio', 'Cruz', 'Bush'])
