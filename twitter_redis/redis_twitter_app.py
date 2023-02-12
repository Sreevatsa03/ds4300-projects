"""
Twitter Database API for Redis
"""

from twitter_objects import Tweet
import time
import redis


class TwitterAPI:

    def __init__(self, host="localhost", port=32769, password="redispw"):
        """ Constructor that creates a connection to redis using connection pool"""

        # Connect to redis
        self.redis = redis.Redis(host=host,
                                 port=port,
                                 password=password,
                                 decode_responses=True)

    def load_followers(self, followers):
        """
        Load followers into redis
        """

        # insert followers into database
        for follower in followers:
            self.redis.sadd(follower[0], follower[1])

    def post_tweet(self, tweet: Tweet):
        """
        Insert tweets into redis
        """

        # post tweet to timeline
        self.post_to_timeline(tweet)

        # get timestamp
        timestamp = time.time()

        # hashtable of tweets
        if self.r.hexists("tweets", "next_tweet_id") != 0:
            # get next tweet id
            next_twt = self.redis.hincrby("tweets", "next_tweet_id", 1)

            # insert tweet into hashtable
            self.redis.hset("tweets", mapping={"tweet_id": next_twt, "user_id": tweet.user_id, "tweet_text": tweet.tweet_text, "timestamp": timestamp})
        else:
            # insert tweet into hashtable
            self.redis.hset("tweets", mapping={"tweet_id": 1, "user_id": tweet.user_id, "tweet_text": tweet.tweet_text, "timestamp": timestamp})

    def post_to_timeline(self, tweet: Tweet):
        """
        Insert tweets into timelines of users that are following the user that posted the tweet
        """

        # get followers of user
        followers = self.r.smembers(tweet.user_id)

        # insert tweet into timelines of followers
        for follower in followers:
            # create timeline key
            timeline = f'Timeline:{follower}'

            # insert tweet into timeline
            self.redis.lpush(timeline, str(tweet))

    def get_timeline(self, user):
        """
        Get timeline of tweets from users a given user is following
        """

        # set timeline key
        timeline_key = f'Timeline:{user}'

        # get tweets from timeline
        timeline = self.redis.lrange(timeline_key, 0, 10)

        # return tweets
        return timeline
