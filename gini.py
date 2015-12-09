from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

import uuid

def gini(values):
    '''
    Source: http://dilumb.blogspot.com/2012/07/python-code-to-calculate-gini.html
    Calculate Gini index, Gini coefficient, Robin Hood index, and points of 
    Lorenz curve based on the instructions given in 
    www.peterrosenmai.com/lorenz-curve-graphing-tool-and-gini-coefficient-calculator
    Lorenz curve values as given as lists of x & y points [[x1, x2], [y1, y2]]
    @param values: List of values
    @return: [Gini index, Gini coefficient, Robin Hood index, [Lorenz curve]] 
    '''
    n = len(values)
    assert(n > 0), 'Empty list of values'
    sortedValues = sorted(values) #Sort smallest to largest

    #Find cumulative totals
    cumm = [0]
    for i in range(n):
        cumm.append(sum(sortedValues[0:(i + 1)]))

    #Calculate Lorenz points
    LorenzPoints = [[], []]
    sumYs = 0           #Some of all y values
    robinHoodIdx = -1   #Robin Hood index max(x_i, y_i)
    for i in range(1, n + 2):
        x = 100.0 * (i - 1)/n
        y = 100.0 * (cumm[i - 1]/float(cumm[n]))
        LorenzPoints[0].append(x)
        LorenzPoints[1].append(y)
        sumYs += y
        maxX_Y = x - y
        if maxX_Y > robinHoodIdx: robinHoodIdx = maxX_Y   
   
    giniIdx = 100 + (100 - 2 * sumYs)/n #Gini index 

    return giniIdx/100


if __name__ == "__main__":

      sc = SparkContext(appName="MDISN")
      sqlContext = SQLContext(sc)
      
      data = sqlContext.read.json("s3n://mdisn/data/paris0.txt")
      #data = sqlContext.read.json("s3n://mdisn/data/twitter-11-06-2012")
      
      # How many results to return
      N = 10

      # Select user columns
      users = data.select(
                data.user.screen_name.alias('user'),
                data.retweeted_status.user.screen_name.alias('other_user')
              ).dropna()

      # For each retweet, count how many retweets
      # are coming from each each other user
      edges = users.map(lambda x: ( (x.user, x.other_user), 1))\
                   .reduceByKey(lambda a,b: a+b)\
                   .map(lambda x: (x[0][1], x[1]))\
                   .groupByKey()
      # Calculate ginni coefficient for each original tweet
      g = edges.map(lambda x: (gini(list(x[1])), x[0]))\
               .filter(lambda x: x[0] > 0.00000001)

      # Print results
      output = g.sortByKey(ascending=False).coalesce(1)
      output.saveAsTextFile('s3n://mdisn/output/gini-paris' + str(uuid.uuid4())[:8])
