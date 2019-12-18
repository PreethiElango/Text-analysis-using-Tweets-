# Import libraries
import tweepy # Twitter
import numpy
import pandas
import matplotlib
from nltk.stem import PorterStemmer
from textblob import TextBlob, Word # Text pre-processing
import re # Regular expressions
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator # Word clouds
from PIL import Image
from pyspark.sql import SparkSession # Spark

# Import Twitter authentication file
from twitter_auth import *

# Twitter authentication
def twitter_auth():
    # This function has been completed for you
    # It uses hardcoded Twitter credentials and returns a request handler
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    return tweepy.API(auth)

# Retrieve Tweets
def get_tweets():
    # This function has been completed for you
    # It creates a Tweet list and extracts Tweets
    account = 'preethielango_'
    extractor = twitter_auth() # Twitter handler object
    tweets = []
    for tweet in tweepy.Cursor(extractor.user_timeline, id = account).items():
        tweets.append(tweet)
    print('Number of Tweets extracted: {}.\n'.format(len(tweets)))
    return tweets

# Create dataframe
def make_dataframe(tweets):
    # This function should return a dataframe containing the text in the Tweets
    tweets_list = []
    tweets[0].__dict__['text']
    for i in range(0,len(tweets)):
        tweets_list.append(tweets[i].__dict__['text'])
	return pandas.DataFrame(data = tweets_list, columns = ['Tweets'])


# Pre-process Tweets
def clean_tweets(data):
    # This function has been completed for you
    # It pre-processes the text in the Tweets and runs in paralle
    print("inside clean tweets")
    print("len of data")
    print(len(data))
    spark = SparkSession\
    .builder\
    .appName("PythonPi")\
    .getOrCreate()
    sc = spark.sparkContext
    paralleled = sc.parallelize(data)
    print("print paralleld")
    print(paralleled)
    return paralleled.map(text_preprocess).collect()

# Pre-process text in Tweet
def text_preprocess(tweet):
    # This function should return a Tweet that consists of only lowercase characters,
    # no hyperlinks or symbols, and has been stemmed or lemmatized
    # Hint: use TextBlob and Word(tweet) and look up which functions you can call
    print("tweet that is passed to textpreprocess")
    print(tweet)
    print("length")
    print(len(tweet))
    p = tweet.lower()
    url_reg  = 'http\S+?:\/\/.[\r\n]'
    r='[^A-Za-z0-9 \s]'
    k='https'
    p = p.replace(url_reg,'')
    p = p.replace(r,'')
    p = p.replace(k,'')
    st = PorterStemmer()
    k=""
    j=""
    for word in p.split():
	    k=st.stem(word)
        j=" "+k
    p=j
    print(p)
    return p

# Retrieve sentiment of Tweets
def generate_sentiment(data):
    # This function has been completed for you
    # It returns the sentiment of the Tweets and runs in parallel
    spark = SparkSession\
    .builder\
    .appName("PythonPi")\
    .getOrCreate()
    sc = spark.sparkContext
    paralleled = sc.parallelize(data)
    return paralleled.map(data_sentiment).collect()

# Retrieve sentiment of Tweet
def data_sentiment(tweet):
    df=[]
    # This function should return 1, 0, or -1 depending on the value of text.sentiment.polarity
    text = TextBlob(tweet)
    if text.sentiment.polarity>0:
        return 1
    elif text.sentiment.polarity==0:
        return 0
    else:
        return -1

# Classify Tweets
def classify_tweets(data):
    # Given the cleaned Tweets and their sentiment,
    # this function should return a list of good, neutral, and bad Tweets
    good_tweets = data[data['sentiment']==1]['cleaned_tweets']
    neutral_tweets = data[data['sentiment']==0]['cleaned_tweets']
    bad_tweets = data[data['sentiment']==-1]['cleaned_tweets']
    print("good tweetts")
    print(good_tweets)
    return [good_tweets, neutral_tweets, bad_tweets]

# Create word cloud
def create_word_cloud(classified_tweets) :
    # Given the list of good, neutral, and bad Tweets,
    # create a word cloud for each list
    # Use different colors for each word cloud
    good_tweets = set(classified_tweets[0])
    neutral_tweets = set(classified_tweets[1])
    bad_tweets =set( classified_tweets[2])
    k=[]
    k.extend(good_tweets)
    good=' '.join(k)
    k=[]
    k.extend(neutral_tweets)
    neutral=' '.join(k)
    k=[]
    k.extend(bad_tweets)
    bad= ' '.join(k)

    stopwords = set(STOPWORDS)
    good_cloud = WordCloud(width = 1800, height = 1500).generate(good)
    neutral_cloud = WordCloud(width = 1800, height = 1500).generate(neutral)
    bad_cloud = WordCloud(width = 1800, height = 1500).generate(bad)
    produce_plot(good_cloud, "Good.png")
    produce_plot(neutral_cloud, "Neutral.png")
    produce_plot(bad_cloud, "Bad.png")

# Produce plot
def produce_plot(cloud, name):
    # This function has been completed for you
    matplotlib.pyplot.axis("off")
    matplotlib.pyplot.imshow(cloud, interpolation='bilinear')
    fig = matplotlib.pyplot.figure(1)
    fig.savefig(name)
    matplotlib.pyplot.clf()

# Task 01: Retrieve Tweets
tweets = get_tweets()
# Task 02: Create dataframe 
df = make_dataframe(tweets)
# Task 03: Pre-process Tweets
df.reset_index()
print(len(df))
#print(df)
#print(clean_tweets(df))
df['cleaned_tweets'] = clean_tweets(df['Tweets'])
print(df.head())
# Task 04: Retrieve sentiments
df['sentiment'] = generate_sentiment(df['cleaned_tweets'])
print(df['sentiment'])
# Task 05: Classify Tweets
classified_tweets = classify_tweets(df)
# Task 06: Create Word Cloud
print("in main print classified")
print(classified_tweets[0])
create_word_cloud(classified_tweets)