"""Utils for parsing the response data from the v2 search endpoint
"""
from enum import Enum
import pandas as pd
import numpy as np


class RType(Enum):
    """Types of user-user and user-content interactions

    Attributes:
        MENTION (int): mention
        QUOTE (int): quote
        REPLY (int): reply
        RETWEET (int): retweet
    """

    RETWEET = 1
    QUOTE = 2
    REPLY = 3
    MENTION = 4


def parse_data(data):
    """
    Args:
        data (list(dict)): from response["data"]

    Returns:
        pd.DataFrame: Description
    """
    df = pd.DataFrame.from_dict(data)
    metrics = df["public_metrics"].apply(pd.Series)

    try:
        ref1 = [parse_ref(t) for t in df["referenced_tweets"]]
        ref2 = pd.DataFrame.from_dict(ref1)
        df = pd.concat([df, metrics, ref2], axis=1)
    except KeyError:
        pass

    return df


def parse_ref(t):
    """
    Args:
        t (dict): dictionary of referenced_tweets

    Returns:
        dict: dictionary mapping replied_to, quoted, and retweeted to the respective tweet_ids
    """
    try:
        a = {"replied_to": "", "quoted": "", "retweeted": ""}
        x = pd.DataFrame(t)
        x = x.set_index("type")
        x = x.T
        x = x.reset_index(drop=True)
        x = x.to_dict(orient="records")
        a.update(x[0])
        return a
    except:
        return {"replied_to": "", "quoted": "", "retweeted": ""}


def parse_users(data):
    """
    Args
        data: list(dict) from response["includes"]["users"]

    Args:
        data (TYPE): Description

    Returns:
        TYPE: Description
    """
    df = pd.DataFrame.from_dict(data)
    metrics = df["public_metrics"].apply(pd.Series)
    df = pd.concat([df, metrics], axis=1)
    return df


def parse_retweets(tweet_id, author_id, retweet_id, tweet_df, referenced_df):
    """
    Args
        tweet_id: str
        author_id: str
        retweet_id: str
        tweet_df: pd.DataFrame
        referenced_df: pd.DataFrame

    Args:
        tweet_id (TYPE): Description
        author_id (TYPE): Description
        retweet_id (TYPE): Description
        tweet_df (TYPE): Description
        referenced_df (TYPE): Description

    Returns:
        TYPE: Description
    """
    try:
        rt = referenced_df.loc[referenced_df.id == retweet_id].author_id.values[0]
        return {
            "src_user_id": rt,
            "tar_user_id": author_id,
            "tweet_id": tweet_id,
            "rtype": RType.RETWEET.value,
        }
    except:
        try:
            rt = tweet_df.loc[referenced_df.id == retweet_id].author_id.values[0]
            return {
                "src_user_id": rt,
                "tar_user_id": author_id,
                "tweet_id": tweet_id,
                "rtype": RType.RETWEET.value,
            }
        except:
            return {}


def parse_quotes(tweet_id, author_id, quote_id, tweet_df, referenced_df):
    """
    Args
        tweet_id: str
        author_id: str
        quote_id: str
        tweet_df: pd.DataFrame
        referenced_df: pd.DataFrame

    Args:
        tweet_id (TYPE): Description
        author_id (TYPE): Description
        quote_id (TYPE): Description
        tweet_df (TYPE): Description
        referenced_df (TYPE): Description

    Returns:
        TYPE: Description
    """
    try:
        quid = referenced_df.loc[referenced_df.id == quote_id].author_id.values[0]
        return {
            "src_user_id": quid,
            "tar_user_id": author_id,
            "tweet_id": tweet_id,
            "rtype": RType.QUOTE.value,
        }
    except:
        try:
            quid = tweet_df.loc[tweet_df.id == quote_id].author_id.values[0]
            return {
                "src_user_id": quid,
                "tar_user_id": author_id,
                "tweet_id": tweet_id,
                "rtype": RType.QUOTE.value,
            }
        except:
            return {}


def parse_replies(tweet_id, author_id, in_reply_to_user_id):
    """
    Args
        tweet_id: str
        author_id: str
        in_reply_to_user_id: str

    Args:
        tweet_id (TYPE): Description
        author_id (TYPE): Description
        in_reply_to_user_id (TYPE): Description

    Returns:
        TYPE: Description
    """
    return {
        "src_user_id": in_reply_to_user_id,
        "tar_user_id": author_id,
        "tweet_id": tweet_id,
        "rtype": RType.REPLY.value,
    }


def parse_mentions(tweet_id, author_id, entities):
    """
    Args
        tweet_id: str
        author_id: str
        entities: str

    Args:
        tweet_id (TYPE): Description
        author_id (TYPE): Description
        entities (TYPE): Description

    Returns:
        TYPE: Description
    """
    mentions = []
    if "mentions" in entities.keys():
        for m in entities["mentions"]:
            mentions.append(
                {
                    "src_user_id": author_id,
                    "tar_user_id": m["id"],
                    "tweet_id": tweet_id,
                    "rtype": RType.MENTION.value,
                }
            )
    return mentions
