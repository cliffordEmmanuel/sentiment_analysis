# this program accesses tweets posted up to 7 days prior per a search query.

from datetime import datetime
import tweepy
import pandas as pd
import time
from credentials import credentials as c


# authentication
auth = tweepy.OAuthHandler(c['consumer_key'], c['consumer_secret'])
auth.set_access_token(c['access_token'],c['access_token_secret'])

# creating the api for use

api = tweepy.API(auth)

# writing a method to get tweets by a search key

def scrapeTweets(search_key, max_tweets) -> 'Dataframe':
    """"downloads a specified number of tweets based on the search key by making a specified number of calls to the standard twitter  search api endpoint."""
    
    tweets_df = pd.DataFrame([])
    since_id = None
    max_id = -1
    tweets_counter = 0

    while tweets_counter < max_tweets:
        if max_id <= 0:
            if not since_id:
                tweets = tweepy.Cursor( api.search, 
                                        q= f'{search_key} -filter:retweets',
                                        lang="en", 
                                        wait_on_rate_limit=True, 
                                        wait_on_rate_limit_notify=True).items(100)
            else:
                tweets = tweepy.Cursor( api.search, 
                                        q= f'{search_key} -filter:retweets',
                                        lang="en",
                                        since_id=since_id,
                                        wait_on_rate_limit=True, 
                                        wait_on_rate_limit_notify=True).items(100)
        else:
            if not since_id:
                tweets = tweepy.Cursor( api.search, 
                                        q= f'{search_key} -filter:retweets',
                                        lang="en", 
                                        max_id=str(max_id -1),
                                        wait_on_rate_limit=True, 
                                        wait_on_rate_limit_notify=True).items(100)
            else:
                tweets = tweepy.Cursor( api.search, 
                                        q= f'{search_key} -filter:retweets',
                                        lang="en",
                                        since_id=since_id, 
                                        max_id=str(max_id -1),
                                        wait_on_rate_limit=True, 
                                        wait_on_rate_limit_notify=True).items(100)

        if not tweets:
            print("No more tweets found")
            break

        # preparing the 
        tweets_list = [tweet for tweet in tweets]

        tweet_created_at_list = []
        tweet_id_list = []
        tweet_text_list = []
        tweet_source_list = []
        tweet_coordinate_list = []
        tweet_retweet_count_list = []
        tweet_likes_count_list = []
        user_id_list = []
        user_name_list = []
        user_screen_name_list = []
        user_location_list = []
        user_followers_count_list = []
        user_friends_count_list = []
        user_created_at_list = []
        user_is_verified_list = []
        user_description_list = []


        for tweet in tweets_list:
            tweet_created_at_list.append(tweet.created_at)
            tweet_id_list.append(tweet.id_str)
            tweet_text_list.append(tweet.text)
            tweet_source_list.append(tweet.source)
            tweet_coordinate_list.append(tweet.coordinates)
            tweet_retweet_count_list.append(tweet.retweet_count)
            tweet_likes_count_list.append(tweet.favorite_count)
            user_id_list.append(tweet.user.id_str)
            user_name_list.append(tweet.user.name)
            user_screen_name_list.append(tweet.user.screen_name)
            user_location_list.append(tweet.user.location)
            user_followers_count_list.append(tweet.user.followers_count)
            user_friends_count_list.append(tweet.user.friends_count)
            user_created_at_list.append(tweet.user.created_at)
            user_is_verified_list.append(tweet.user.verified)
            user_description_list.append(tweet.user.description)


        single_run_df = pd.DataFrame( {
            "tweet_created_at": tweet_created_at_list,
            "tweet_id" : tweet_id_list,
            "tweet_text" : tweet_text_list,
            "tweet_source" : tweet_source_list,
            "tweet_coordinate" : tweet_coordinate_list,
            "tweet_retweet_count" : tweet_retweet_count_list,
            "tweet_likes_count" : tweet_likes_count_list,
            "user_id_list" : user_id_list,
            "user_name_list": user_name_list,
            "user_screen_name_list": user_screen_name_list,
            "user_location_list": user_location_list,
            "user_followers_count_list": user_followers_count_list,
            "user_friends_count_list": user_friends_count_list,
            "user_created_at_list": user_created_at_list,
            "user_is_verified_list": user_is_verified_list,
            "user_description_list": user_description_list
        })
        # call_end_time = time.time()

        tweets_df = pd.concat([tweets_df,single_run_df])
        # print(f"{len(tweet_id_list)} tweets downloaded in request {request+1} in {round(call_end_time-call_start_time)} seconds")

        # except tweepy.TweepError as e: 
        #     print(f"Tweepy Error: {e}")
        #     continue
    return tweets_df


    

# the main function call

if __name__ == '__main__':

    search_key = "Bitcoin" 
    run_start = time.time()
    result = scrapeTweets(search_key,200)
    run_end = time.time()

    print(f"*******************************************************")
    print(f"Script took {round((run_end-run_start)/60,2)} mins to download {len(result.tweet_id)} tweets!!")
    print(f"Number of unique tweets: {result.tweet_id.nunique()}")
    print(f"Number of duplicate tweets: {result.shape[0]-result.tweet_id.nunique()}")
    print(f"Dataframe size: {result.shape}")
    print(f"Timestamp for oldest tweet downloaded: {result.tweet_created_at.min()}")
    print(f"*******************************************************")