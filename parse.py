"""Utils for parsing the response data from the v2 search endpoint
"""
from enum import Enum
import pandas as pd
import numpy as np
import logging

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


def parse_users(response, logger, file=None):
    """
    Args:
        response (json): the entire response json
        logger (logger): logger
        file (None, optional): filename of response if reading from json
    
    Returns:
        pd.DataFrame: user dataframe
    """
    try:
        data = response["includes"]["users"]
    except KeyError:
        msg = "KeyError: no users in response" if (file is None) else f"KeyError: no users in response {file}"
        logger.info(msg)
        return pd.DataFrame()
    
    df = pd.DataFrame.from_dict(data)
    metrics = df["public_metrics"].apply(pd.Series)
    df = pd.concat([df, metrics], axis=1)
    return df

def parse_media(response, logger, file=None):
    """
    Args:
        response (json): the entire response json
        logger (logger): logger
        file (None, optional): filename of response if reading from json
    
    Returns:
        pd.DataFrame: media dataframe
    """
    try:
        data = response["includes"]["media"]
    except KeyError:
        msg = "KeyError: no media in response" if (file is None) else f"KeyError: no media in response {file}"
        logger.info(msg)
        return pd.DataFrame()

    return pd.DataFrame.from_dict(data)

def concat_and_pickle(df_list, df_name, pickle_path, pickle_protocol):
    """
    
    Args:
        df_list (list(pd.DataFrame)): list of dataframes to aggregate
        df_name (str): the type of data being saved
        pickle_path (str): path to save pickle to
        pickle_protocol (int): pickle protocol to use
    
    Returns:
        None
    """
    try:
        df = pd.concat(df_list)
    except:
        logger.warning(f"Cannot concat {df_name} list of {len(df_list)} dataframes")
        return

    try:
        df.to_pickle(pickle_path, protocol=pickle_protocol)
        logger.info(f"Saved {df_name} to {pickle_path}")
    except:
        logger.warning(f"Cannot save {df_name} to {pickle_path}")

def read_aggregate_pickle(cache_dir, save_dir, logger, agg_interval=1000, pickle_protocol=4, debug_mode=False):
    """Read cached intermediate json files -> aggregate and pickle as dataframes
    
    Args:
        cache_dir (str): directory of cached intermediate json files
        save_dir (str): directory to write pickled dataframes to
        logger (logger): logger
        agg_interval (int, optional): interval at which to concatenate results and pickle
        pickle_protocol (int, optional): pickle protocol to use
        debug_mode (bool, optional): run on the first few files
    """
    all_tweets, all_users, all_media, all_ref = [], [], [], []
    files = [f for f in os.listdir(cache_dir) if f.endswith("json")]
    if debug_mode:
        files = files[:10]
    logger.info(f"{len(files)} files to process")
    not_loaded = []
    start = time.time()
    for i,file in enumerate(files):
        try:
            with open(osp.join(cache_dir, file), "r") as handle:
                res = json.load(handle)
            try:
                tweets = parse_data(res["data"])
                all_tweets.append(tweets)
                users = parse_users(res)
                all_users.append(users)
                media = parse_media(res)
                all_media.append(media)
                try:
                    ref = parse_data(res["includes"]["tweets"])
                    all_ref.append(ref)
                except KeyError:
                    logger.info(f"KeyError: no included/referenced tweets in response {file}")
        except:
            logger.warning(f"Cannot load json {file} - pass")
            not_loaded.append(osp.join(cache_dir, file))
            pass

        if (i%agg_interval == 0 and i > 0) or (i == len(files)-1):
            logger.info(f"Iter {i}: {str(round(time.time()-start, 2))} s elapsed")
            concat_and_pickle(all_tweets, "tweets", osp.join(save_dir, f"tweets_{i}.pickle"), pickle_protocol)
            concat_and_pickle(all_users, "users", osp.join(save_dir, f"users_{i}.pickle"), pickle_protocol)
            concat_and_pickle(all_media, "media", osp.join(save_dir, f"media_{i}.pickle"), pickle_protocol)
            concat_and_pickle(all_ref, "ref", osp.join(save_dir, f"ref_{i}.pickle"), pickle_protocol)
