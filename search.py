"""Utils for full archive search
"""
import json
import numpy as np
import os
import os.path as osp
import pandas as pd
import requests
import time

import config  # credentials
import parse


def auth():
    """Get auth

    Returns:
        str: bearer token
    """
    return config.bearer_token


def create_search_url(next_token=None):
    """Get search url

    Args:
        next_token (None, optional): next_token for pagination

    Returns:
        str: search url
    """
    if next_token:
        return f"https://api.twitter.com/2/tweets/search/all?next_token={next_token}"
    else:
        return "https://api.twitter.com/2/tweets/search/all"


def search_request(
    query,
    start_time,
    max_results=500,
    next_token=None,
):
    """Make and send request

    Args:
        query (str): search query
        start_time (str): start time
        max_results (int, optional): max number of search results to be returned by a request, between 10 (system default) and 500 (system limit)
        next_token (None, optional): token to get the next page of results

    Returns:
        json: response
    """
    # authenticate
    token = auth()
    # headers
    headers = {
        "content-type": "application/json",
        "authorization": f"Bearer {token}",
    }
    # url
    url = create_search_url(next_token)
    # params
    params = {
        "query": query,
        "tweet.fields": "id,text,author_id,conversation_id,entities,in_reply_to_user_id,referenced_tweets,attachments,created_at,public_metrics",
        "expansions": "attachments.media_keys,author_id,in_reply_to_user_id,referenced_tweets.id.author_id",
        "media.fields": "media_key,type,url",
        "start_time": start_time,
        "user.fields": "id,location,name,public_metrics,url,username,verified",
        "max_results": max_results,
    }
    # make request
    response = requests.request("GET", url, headers=headers, params=params)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)

    return response.json()


def get_results(query, start_time, next_token=None, write_params=None):
    """Summary

    Args:
        query (str): follow twitter guidelines for building query: https://developer.twitter.com/en/docs/twitter-api/tweets/counts/integrate/build-a-query
        start_time (str):  start date in ISO 8601 format (YYYY-MM-DDTHH:mm:ssZ). To search from the beginning, use "2006-03-21T00:00:00Z"
        next_token (None, optional): token to get the next page of results
        write_params (None, optional): dictionary with save_dir, save_name if you want to write the intermediate responses to JSON

    Returns:
        pd.DataFrame: dataframe of aggregate results
    """
    count = 0
    page = 0
    tweet_df = pd.DataFrame()

    while True:
        try:
            res = search_request(query, start_time, next_token=next_token)
            result_count = res["meta"]["result_count"]
            count += result_count
            page += 1
        except Exception as e:
            print(e)
            print(f"page {page} :: num_results {count}")
            print(f"next_token: {next_token}")
            return tweet_df

        if write_params:
            assert("save_dir" in write_params)
            assert("save_name" in write_params)
            tname = 0 if (next_token is None) else next_token
            save_file = osp.join(write_params["save_dir"], f"{write_params["save_name"]}_{page}_{tname}.json")
            with open(save_file, "w") as handle:
                json.dump(res, handle)
                print(f"Wrote result to {save_file}.")

        d = parse.parse_data(res)
        tweet_df = tweet_df.append(d)

        time.sleep(3)  # rate limit effectively 3 sec/request

        if "next_token" in res["meta"]:
            next_token = res["meta"]["next_token"]
        else:
            break

    tweet_df = tweet_df.reset_index(drop=True)
    # some maintenance
    tweet_df = tweet_df.replace(to_replace=[r"^\s*$", None], value=np.nan, regex=True)
    tweet_df["tweet_id"] = tweet_df["id"].astype(int).astype(str)
    tweet_df["author_id"] = tweet_df["author_id"].astype(int).astype(str)
    tweet_df["in_reply_to_user_id"] = (
        tweet_df["in_reply_to_user_id"].astype(int).astype(str)
    )
    tweet_df["conversation_id"] = tweet_df["conversation_id"].astype(int).astype(str)
    tweet_df["replied_to"] = tweet_df["replied_to"].astype(int).astype(str)
    tweet_df["quoted"] = tweet_df["quoted"].astype(int).astype(str)
    tweet_df["retweeted"] = tweet_df["retweeted"].astype(int).astype(str)
    return tweet_df
