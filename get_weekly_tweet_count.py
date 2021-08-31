import twint
import pandas as pd
from datetime import datetime, timedelta
from pymongo import MongoClient
from time import sleep
from decouple import config as env_config

pd.options.mode.chained_assignment = None


MONGO_URL = env_config("MONGO_URL")

client = MongoClient(MONGO_URL)

db = client.twitter_users
collection = db.weekly_count


today = datetime.now()
start_date = today + timedelta(-7)
end_date = today + timedelta(-1)


start_date_str = start_date.strftime("%Y-%m-%d")
end_date_str = end_date.strftime("%Y-%m-%d")


def twint_to_pandas(columns):
    return twint.output.panda.Tweets_df[columns]


def get_latest_tweets_from_handle(username, num_tweets,start_date,  end_date):

    c = twint.Config()
    c.Username = username
    c.Limit = num_tweets
    c.Pandas = True
    c.Since = start_date
    c.Until = end_date
    c.Hide_output = True
    twint.run.Search(c)
    try:
        tweet_df = twint_to_pandas(['id', 'conversation_id', 'date', 'tweet', 'language', 'hashtags',
                                    'username', 'name', 'link', 'urls', 'photos', 'video',
                                    'thumbnail', 'retweet', 'nlikes', 'nreplies', 'nretweets', 'source'])
    except Exception as e:
        print(e)
        tweet_df = pd.DataFrame()

    tweet_count = len(tweet_df)

    ave_replies = round(sum(tweet_df['nreplies'].tolist()) / tweet_count)

    return tweet_count, ave_replies 


def get_record_details(search_dict, collection, find_one=True):
    try:
        query = collection.find_one(search_dict) if find_one else collection.find(search_dict)
        return query
    except Exception as e:
        print(e)
        return None


def insert_records(collection, record):
    try:
        collection.insert_one(record)
    except Exception as e:
        print(e)

def save_to_mongo_db(data):
    insert_records(collection, data)
    cur = collection.count_documents({})
    print(f"we have {cur} entries")

def get_weekly_count(username):

    try:
        tweet_count, ave_replies = get_latest_tweets_from_handle(username, 2500, start_date_str, end_date_str)
    except Exception as e:
        tweet_count = 0
        ave_replies = 0
        print(e)

    data = {
        'username' : username,
        'num_of_tweets' :  tweet_count,
        'ave_replies': ave_replies,
        'start_date': start_date_str,
        'end_date': end_date_str
    }    

    save_to_mongo_db(data)
    return None

def run_script():
    try:
        twitter_users_df = pd.read_csv("twitter_details_collections.csv", dtype="unicode")
        usernames = twitter_users_df['handle'].tolist()

        for username in usernames[:3]:
            print(username)
            search_dict = {'username': username}
            query = get_record_details(search_dict, collection, find_one=True)

            if query == None:
                get_weekly_count(username) 
                sleep(10)

    except Exception as e:
        print(e)


run_script()