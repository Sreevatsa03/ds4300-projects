"""
TwitterAPI driver to call functions that post tweets and get timelines
"""

from redis_twitter_app import TwitterAPI
from twitter_objects import Tweet
import random as rnd
import time

class TwitterAPIDriver:
    """ Driver class to call functions that post tweets and get timelines """

    def __init__(self, host, port, password):
        """ API Driver constructor """

        # Create a TwitterAPI object
        self.twitter = TwitterAPI(host, port, password)

    def load_followers(self, filename):
        """
        Load followers into database

        :param filename: name of the CSV file
        :type filename: str
        """

        # dictionary to store followers
        follows = {}

        # read csv
        with open(filename) as followers:
            followers.readline()

            # iterate through each line
            for line in followers:
                # split line into list
                line = line.strip().split(',')

                # add follower to database
                user, follower = line[0], line[1]

                # add follower to dictionary
                if follower in follows.keys():
                    follows[follower].append(user)
                else:
                    follows[follower] = [user]

        # get count of users
        num_users = len(follows.keys())

        # Call the load_followers function
        for user, followers in follows.items():
            self.twitter.load_followers(user, followers)

        return num_users

    def post_tweets(self, filename):
        """ 
        Post tweets to database
            
        :param tweets: batch of tweets to post
        :type tweets: list[Tweet]
        """

        # read csv
        with open(filename) as tweets:

            # iterate through each line
            for line in tweets:
                # split line into list
                line = line.strip().split(',')

                # post tweet
                self.twitter.post_tweet(Tweet(line[0], line[1]))

        # Call the post_tweet function
        self.twitter.post_tweet(tweets)

    def get_timelines(self, user_num, time_limit=10) -> int:
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
            user_id = rnd.randint(0, user_num)

            # get timeline
            timeline = self.twitter.get_timeline(user_id)

            # print timeline
            print(user_id + " : " + timeline)

            # increment counter
            counter += 1

        return counter
        

    def shutdown(self):
        """ Close connection to db """

        self.twitter.shutdown()


def main():
    """ Main function """

    # Create a TwitterAPI object user port from docker redis container
    twitter = TwitterAPIDriver(host='localhost', port=32769, password='redispw')

    # Call the load_followers function
    user_num = twitter.load_followers('data/followers.csv')

    # time how long it takes to post all tweets
    start_time = time.time()

    # post tweets
    twitter.post_tweets('data/tweet.csv')

    # print time it took to post all tweets
    print("Time to post all tweets: ", time.time() - start_time)

    # Call the get_timeline function
    time = 10
    timeline_num = twitter.get_timelines(user_num, time)
    print(timeline_num / time, " timelines retrieved per second")

if __name__ == "__main__":
    main()
