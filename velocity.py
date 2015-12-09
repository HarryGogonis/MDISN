from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

import uuid, re
from datetime import *
from dateutil import parser as dt

# This is analogous to python's 
# timedelta.total_seconds() method
# which is missing from python v2.6
def total_seconds(timedelta):
  return (timedelta.microseconds + 0.0 +
         (timedelta.seconds + timedelta.days * 24 * 3600) * 10 ** 6) / 10 ** 6

# The velocity of a tweet
def getWeight(source, dest, retweet_count):
  diff = dt.parse(source) - dt.parse(dest)
  return retweet_count / total_seconds(diff)

# Strip out RT @user from tweet's text
def parseTweet(text):
  return re.sub(r'RT @.+: ','',text)

if __name__ == "__main__":

      sc = SparkContext(appName="MDISN")
      sqlContext = SQLContext(sc)
      
      #data = sqlContext.read.json("s3n://mdisn/data/paris0.txt")
      data = sqlContext.read.json("s3n://mdisn/data/election")
      
      # How many results to return
      N = 10
      # Threashold value
      # Tweets with less than this amount of retweets
      # Will not be considered
      tau = 1000

      # Only return tweets that fit into threashold
      users = data.filter(data.retweeted_status.retweet_count > tau)

      # Select a few columns
      users = users.select(
                data.user.screen_name.alias('user'),
                data.retweeted_status.user.screen_name.alias('other_user'),
                data.created_at.alias('source_time'),
                data.text,
                data.retweeted_status.retweet_count.alias('retweet_count'),
                data.retweeted_status.created_at.alias('dest_time')
              ).dropna()
  
      # There's a weighted edge between retweeter->source
      # This weight is the tweet's 'velocity'
      edges = users.map(lambda x: (x.other_user + ':' + parseTweet(x.text),\
                                    getWeight(x.source_time, x.dest_time, x.retweet_count)))\

      # Calcuate the average velocity
      # or, the average weight of all
      # edges going into a sink
      avg_velocity = edges.aggregateByKey((0,0),
                                    lambda a,b: (a[0] + b,    a[1] + 1),
                                    lambda a,b: (a[0] + b[0], a[1] + b[1]))\
                          .mapValues(lambda v: v[0]/v[1]).map(lambda (k,v): (v,k))
      
      # Sort final output and save to file
      output = avg_velocity.sortByKey(ascending=False)
      output.coalesce(1).saveAsTextFile('s3n://mdisn/output/velocity-election' + str(uuid.uuid4())[:8])
