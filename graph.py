from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import *

from datetime import *
from dateutil import parser as dt

def total_seconds(timedelta):
  return (timedelta.microseconds + 0.0 +
         (timedelta.seconds + timedelta.days * 24 * 3600) * 10 ** 6) / 10 ** 6

def getWeight(source, dest, retweet_count):
  diff = dt.parse(source) - dt.parse(dest)
  return str(retweet_count / total_seconds(diff))

if __name__ == "__main__":

      sc = SparkContext(appName="MDISN")
      sqlContext = SQLContext(sc)
      
      data = sqlContext.read.json("s3n://mdisn/data/paris0.txt")
      
      # Variables used for formatting output
      nodeLeftDelim = '\tnode\n\t[\n\t\t'
      nodeRightDelim = '"\n\t]'

      edgeLeftDelim = '\tedge\n\t[\n\t\t'
      edgeRightDelim = '\n\t]'

      header = sc.parallelize(['graph\n['])
      footer = sc.parallelize([']'])

      # Get users/edges from data
      users = data.filter(data.retweeted_status.retweet_count > 1000).select(
                data.user.screen_name.alias('user'),
                data.retweeted_status.user.screen_name.alias('other_user'),
                data.created_at.alias('source_time'),
                data.retweeted_status.created_at.alias('dest_time'),
                data.retweeted_status.retweet_count.alias('retweet_count')
              ).dropna()

      edges = users.map(lambda x: edgeLeftDelim + 'source ' + str(x.user) +
                        '\n\t\ttarget ' + str(x.other_user) + edgeRightDelim)

      nodes = users.map(lambda x: nodeLeftDelim + 'id ' + str(x.other_user) +
                        '\n\t\tlabel "' + str(x.other_user) + nodeRightDelim) +\
              users.map(lambda x: nodeLeftDelim + 'id ' + str(x.user) +
                        '\n\t\tlabel "' + str(x.user) + nodeRightDelim)


      # Output data in Gephi format
      output = header + nodes + edges + footer
      output.coalesce(1).saveAsTextFile('s3n://mdisn/output/twitter-graph' + 
                                        datetime.now().strftime('%s') + '.gml')
