"""
Twitter Database API for PostgreSQL
"""

from dbutils import DBUtils
from twitter_objects import Tweet
import datetime
import re

class TwitterAPI:

    def __init__(self):
        self.db = DBUtils()

    def authenticate(self, user, password, database, host="localhost", port="5432"):
        """ Authenticate credentials to access database """

        self.db.authenticate(user, password, database, host, port)

    def shutdown(self):
        """ Close connection to db """

        self.db.close()

    def post_tweet(self, tweets: list[Tweet]):
        """
        Insert batch of tweets into database

        :param tweets: batch of tweets to insert
        :type tweets: list[Tweet]
        """

        # SQL statement to insert tweets
        sql = 'INSERT INTO "TWEET" (user_id, tweet_text) VALUES '

        # convert tweet objects to tuples
        tweets_values = [(int(tweet.user_id), str(tweet.tweet_text)) for tweet in tweets]

        # Insert tweets into database
        self.db.insert_many(sql, tweets_values)

    def _get_followers(self, user_id: int) -> list[int]:
        """
        Get list of followers following a given user

        :param user_id: user id
        :type user_id: int
        :return: list of followers
        :rtype: list[int]
        """

        # SQL statement to get user_id given a follower_id
        sql = 'SELECT user_id FROM "FOLLOWS" WHERE follows_id = ' + str(user_id)

        # Get followers
        following = self.db.execute(sql, df=False)[0]

        # Return list of following users
        return [follow[0] for follow in following]

    def _get_following(self, user_id: int) -> list[int]:
        """
        Get list of users a given user is following

        :param user_id: user id
        :type user_id: int
        :return: list of users being followed by a given user
        :rtype: list[int]
        """

        # SQL statement to get followers
        sql = 'SELECT follows_id FROM "FOLLOWS" WHERE user_id = ' + str(user_id)

        # Get followers
        followers = self.db.execute(sql, df=False)[0]

        # Return list of followers
        return [follower[0] for follower in followers]

    def _parse_timestamp(self, timestamp):
        """ 
        Parse timestamp into datetime object
        
        :param timestamp: timestamp
        :type timestamp: str
        :return: datetime object
        :rtype: datetime
        """
        
        return datetime.datetime(*[int(x) for x in re.findall(r'\d+', timestamp)])

    def _get_tweets(self, user_id: int) -> list[Tweet]:
        """
        Get list of tweets from a given user

        :param user_id: user id
        :type user_id: int
        :return: list of tweets
        :rtype: list[Tweet]
        """

        # SQL statement to get tweets from a given user
        sql = 'SELECT tweet_id, user_id, tweet_ts, tweet_text FROM "TWEET" WHERE user_id = ' + str(user_id)

        # Get tweets
        tweets = self.db.execute(sql, df=False)[0]

        # Return list of tweets
        return [Tweet(int(tweet[0]), int(tweet[1]), self._parse_timestamp(str(tweet[2])), str(tweet[3])) for tweet in tweets]

    def get_random_user_id(self) -> int:
        """
        Get random user id

        :return: user id
        :rtype: int
        """

        # SQL statement to get random user id
        sql = 'SELECT user_id FROM "FOLLOWS" ORDER BY RANDOM() LIMIT 1'

        # Get user id
        user_id = self.db.execute(sql, df=False)[0][0][0]

        # Return user id
        return int(user_id)

    def get_timeline(self, user_id: int) -> list[Tweet]:
        """
        Get timeline of tweets from users a given user is following

        :param user_id: user id
        :type user_id: int
        :return: timeline as list of tweets
        :rtype: list[Tweet]
        """

        # get 10 most recent tweets posted by any user that that user follows
        # SQL statement to get tweets from users a given user is following
        sql = 'SELECT tweet_id, user_id, tweet_ts, tweet_text FROM "TWEET" WHERE user_id IN (SELECT follows_id FROM "FOLLOWS" WHERE user_id = ' + str(user_id) + ') ORDER BY tweet_ts DESC LIMIT 10'

        # Get tweets
        tweets = self.db.execute(sql, df=False)[0]

        # Return list of tweets
        return [Tweet(int(tweet[0]), int(tweet[1]), self._parse_timestamp(str(tweet[2])), str(tweet[3])) for tweet in tweets]
