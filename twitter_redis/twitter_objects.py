"""
Twitter objects are used to represent the tweet data that is inserted and returned from the Twitter API
"""

class Tweet(object):
    """ Tweet object """
    def __init__(self, user_id, tweet_text):
        self.user_id = user_id
        self.tweet_text = tweet_text

    def __str__(self):
        return f'User{self.user_id}: {self.tweet_text}'