import pymongo
from credentials import mongodb_keys
from pprint import pprint

def createConnection(username=test, password):
    try :
        client = pymongo.MongoClient(f"mongodb+srv://{username}:{mongodb_keys['password']}@data-io-cluster.ntgty.mongodb.net/retryWrites=true&w=majority")
        return client
    except :
        return "Connection not successful"    

def createCollection():
    pass

def deleteCollection():
    pass

def getCollectionNames(client):
    """returns all the databases present in the database"""
    databases_names = []
    for db in client.list_databases():
        collections.append(db['names']) 
    return database_names

def uploadRawTweets(client, df):
    """upload tweets that have been downloaded"""
    tweets_db = client["tweets_data"]
    raw_tweets = tweets_db["raw_tweets"]
    raw_tweets.insert_many(df)
    print("Done")

def retrieveRawTweets(client, date_uploaded):
    """retrieves raw tweets extracted for a specific day"""
    tweets_db = client["tweets_data"] 
    raw_tweets = test_db["raw_tweets"]
    return raw_tweets.find({"date_uploaded": date_uploaded})

def uploadProcessedTweets(client, df):
    """uploads processed tweets"""
    tweets_db = client["tweets_data"] 
    processed_tweets = tweets_db["processed_tweets"]
    processed_tweets.insert_many(df)
    print("Done")
