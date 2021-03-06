import sys
import re
import json
import pandas as pd
import matplotlib.pyplot as plt

def word_in_text(word, text):
    '''Returns true if the string word is contained in the string text'''
    return True if re.search(word.lower(), text.lower()) is not None else False

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please supply path to data in argument")
        sys.exit()
    
    data_path = sys.argv[1]

    tweets_data = []
    # Read each line, append to tweets_data array    
    with open(data_path, 'r') as tweets_file:
        for line in tweets_file:
            try:
                tweet = json.loads(line)
                if 'text' in tweet:
                    tweets_data.append(json.loads(line))
            except:
                continue
        #tweets_data = [json.loads(line) for line in tweets_file]

    # Create a data frame for our data
    tweets = pd.DataFrame()
    tweets['text'] = map(lambda tweet: tweet['text'], tweets_data)
    tweets['created_at'] = map(lambda tweet: tweet['created_at'], tweets_data)
    tweets['user_id'] = map(lambda tweet: tweet['user']['id'], tweets_data)
    tweets['user_name'] = map(lambda tweet: tweet['user']['name'], tweets_data)
    tweets['retweets'] = map(lambda tweet: tweet['retweet_count'], tweets_data)
    tweets['favorites'] = map(lambda tweet: tweet['favorite_count'], tweets_data)

    # Find keywords
    tweets['Clinton'] = tweets['text'].apply(lambda tweet: word_in_text('Clinton', tweet))
    tweets['Sanders'] = tweets['text'].apply(lambda tweet: word_in_text('Sanders', tweet))
    tweets['Trump'] = tweets['text'].apply(lambda tweet: word_in_text('Trump', tweet))
    tweets['Carson'] = tweets['text'].apply(lambda tweet: word_in_text('Carson', tweet))
    tweets['Rubio'] = tweets['text'].apply(lambda tweet: word_in_text('Rubio', tweet))
    tweets['Cruz'] = tweets['text'].apply(lambda tweet: word_in_text('Cruz', tweet))
    tweets['Bush'] = tweets['text'].apply(lambda tweet: word_in_text('Bush', tweet))

    # Aggregate count of tweets containing each candidate
    candidates =['Clinton', 'Sanders', 'Trump', 'Carson', 'Rubio', 'Cruz', 'Bush']
    tweets_by_candidate = [len(tweets[tweets['Clinton']]),
                        len(tweets[tweets['Sanders']]), 
                        len(tweets[tweets['Trump']]), 
                        len(tweets[tweets['Carson']]), 
                        len(tweets[tweets['Rubio']]), 
                        len(tweets[tweets['Cruz']]), 
                        len(tweets[tweets['Bush']])]

    # Plot
    x_pos = list(range(len(candidates)))
    width = 0.8
    fig, ax = plt.subplots()
    plt.bar(x_pos, tweets_by_candidate, width, alpha=1, color='g')
    
    # Setup axis labels
    ax.set_ylabel('Number of tweets', fontsize=15)
    ax.set_title('Number of tweets containing names of popular 2016 election candidates (sample data)', fontsize=10, fontweight='bold')
    ax.set_xticks([p+0.5*width for p in x_pos])
    ax.set_xticklabels(candidates)
    plt.grid()
    plt.show()
