import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs


def run_twitter_etl():
    access_key= "d6fypXKlx2rCtBFTP2B35jiwU"
    access_secret= "HmnJQYHq1pLLM2rLKaiZht7n19R03eonXtA0RxbjpEQjePwIka"
    consumer_key= "4916643860-BP5YRJHEcbGF5TangO1FIl2vbMLV3L5r45UcKZk"
    consumer_secret= "7KvtyHMdbGfRHXY7HPuVl9A8dyYy7pBbca2DzNq52Yx9l"

    #Twitter authentication
    auth=tweepy.OAuthHandler(access_key,access_secret)
    auth.set_access_token(consumer_key,consumer_secret)

    #Creating an API Object
    api=tweepy.API(auth)

    tweets=api.user_timeline(screen_name='@elonmusk',
                            count=200,
                            include_rts=False,
                            tweet_mode='extended')

    tweet_list=[]
    for tweet in tweets:
        text=tweet._json["full_text"]
        refined_tweet={"user":tweet.user.screen_name,
                        "text":text,
                        "favorite_count":tweet.favorite_count,
                        "retweet_count":tweet.retweet_count,
                        "created_at":tweet.created_at}
        tweet_list.append(refined_tweet)
    df=pd.DataFrame(tweet_list)
    df.to_csv("s3://siva-airflow-bucket/elonmusk_twitter_data.csv")