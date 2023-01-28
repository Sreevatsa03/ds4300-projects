"""
TwitterAPI driver to call functions that post tweets and get timelines
"""

from twitter_app import TwitterAPI
from twitter_objects import Tweet
import pandas
import time

class TwitterAPIDriver:
    """ Driver class to call functions that post tweets and get timelines """

    def __init__(self, user, password, database, host="localhost", port="5432"):
        """ Constructor """

        # Create a TwitterAPI object
        self.twitter = TwitterAPI()

        # Authenticate credentials to access database
        self.twitter.authenticate(user, password, database, host, port)

    def read_tweets(self, filename, batch_size=5) -> list[list[Tweet]]:
        """
        Read tweets from a CSV file and batches them into a list of lists of Tweets

        :param filename: name of the CSV file
        :type filename: str
        :param batch_size: number of tweets per batch  
        :type batch_size: int
        :return: list of batches of Tweets
        :rtype: list[list[Tweet]]
        """

        # read csv into dataframe
        tweets_df = pandas.read_csv(filename)

        # convert dataframe to list of tuples
        tweets = list(tweets_df.to_records(index=False))

        # convert tuple within list to Tweet object
        tweets = [Tweet(None, tweet[0], None, tweet[1]) for tweet in tweets]
        
        # batch tweets into lists of 5
        batches = [tweets[i:i + batch_size] for i in range(0, len(tweets), batch_size)]

        return batches

    def post_tweet(self, tweets: list[Tweet]):
        """ 
        Post tweets to database
            
        :param tweets: batch of tweets to post
        :type tweets: list[Tweet]
        """

        # Call the post_tweet function
        self.twitter.post_tweet(tweets)

    def get_timeline(self, time_limit=10) -> int:
        """ 
        Repeatedly pick random user and get their timeline. Return the number of times a user's timeline was retrieved.
        
        :param user_id: user id
        :type user_id: int
        :param time_limit: time limit in seconds
        :type time_limit: int
        :return: number of timelines retrieved
        :rtype: int
        """

        # iterate for 10 seconds
        start_time = time.time()
        end_time = start_time + time_limit

        # counter for number of timelines retrieved
        counter = 0

        # loop until 10 seconds have passed
        while time.time() < end_time:
            # get random user id
            user_id = self.twitter.get_random_user_id()

            # get timeline
            self.twitter.get_timeline(user_id)

            # increment counter
            counter += 1

        return counter
        

    def shutdown(self):
        """ Close connection to db """

        self.twitter.shutdown()


def main():
    """ Main function """

    # Create a TwitterAPI object
    twitter = TwitterAPIDriver(database="postgres",
                               user='postgres',
                               password='postgres',
                               host='localhost',
                               port='5432')

    # Call the read_csv function
    batched_tweets = twitter.read_tweets('data/tweet.csv')

    # time how long it takes to post all tweets
    start_time = time.time()

    # Call the post_tweet function on each batch
    for batch in batched_tweets:
        twitter.post_tweet(batch)

    # print time it took to post all tweets
    print("Time to post all tweets: ", time.time() - start_time)

    # Call the get_timeline function
    timeline_num = twitter.get_timeline(2)
    print(timeline_num / 10, " timelines retrieved per second")

    # Close the connection
    twitter.shutdown()

if __name__ == "__main__":
    main()
