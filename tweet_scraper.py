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

def scrapeTweets(search_key, number_of_requests, date_until) -> 'Dataframe':
    """"downloads a specified number of tweets based on the search key by making a specified number of calls to the standard twitter  search api endpoint."""
    
    tweets_df = pd.DataFrame([])
    tweets_counter = 0

    # getting the first oldest tweet to work around the pagination.
    tweets = tweepy.Cursor( api.search, 
                                q= f'{search_key} -filter:retweets',
                                lang="en", 
                                until= date_until).items(100)
    
    id_ = max([tweet.id_str for tweet in tweets])

    for request in range(number_of_requests):
        try:
            call_start_time = time.time() 
            # this returns tweets bundled into a iterator object
            tweets = tweepy.Cursor( api.search, 
                                    q=f'{search_key} -filter:retweets',
                                    lang="en", 
                                    until=date_until,
                                    wait_on_rate_limit=True, 
                                    wait_on_rate_limit_notify=True,
                                    max_id=id_).items(100)

            tweets_list = [tweet for tweet in tweets]
            id_ = tweets_list[-1].id_str

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
                tweets_counter += 1
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
            call_end_time = time.time()
            tweets_df = pd.concat([tweets_df,single_run_df])
            print(f"{len(tweet_id_list)} tweets downloaded in request {request+1} in {round(call_end_time-call_start_time)} seconds")
        except tweepy.TweepError as e: 
            print(f"Tweepy Error: {e}")
            continue
    return tweets_df

def scrapeDailyTweets(search_query, day):
    """downloads all tweets per a specific search_query for a single day"""
    pass

# the main function call

if __name__ == '__main__':

    search_key = "iPhone 13" 
    run_start = time.time()
    result = scrapeTweets(search_key,100,'2021-02-24')
    run_end = time.time()

    print(f"*******************************************************")
    print(f"Script took {round((run_end-run_start)/60,2)} mins to download {len(result.tweet_id)} tweets!!")
    print(f"Number of unique tweets: {result.tweet_id.nunique()}")
    print(f"Number of duplicate tweets: {result.shape[0]-result.tweet_id.nunique()}")
    print(f"Dataframe size: {result.shape}")
    print(f"Timestamp for oldest tweet downloaded: {result.tweet_created_at.min()}")
    print(f"*******************************************************")