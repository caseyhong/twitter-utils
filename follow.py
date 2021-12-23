import requests
import json
from datetime import datetime
from pytz import timezone

import config


def request_following(uid, next_token=None):
    """Helper function

    Args:
        uid (str): user id
        next_token (None, optional): next token for pagination

    Returns:
        json: response

    Raises:
        Exception: if request status not 200
    """
    url = f"https://api.twitter.com/2/users/{uid}/following"
    headers = {
        "content-type": "application/json",
        "authorization": f"Bearer {config.bearer_token}",
    }
    params = {"max_results": 1000}

    if next_token:
        params["pagination_token"] = next_token
    response = requests.request("GET", url, headers=headers, params=params)

    if response.status_code != 200:
        raise Exception(response.status_code, response.text)

    return response.json()


def get_following(uid, logger, next_token=None):
    """Get info for the accounts uid is following

    Args:
        uid (str): user id
        logger (logger): logger
        next_token (None, optional): next token for pagination

    Returns:
        tuple(list(dict), int): list of data for the accounts uid is following
    """
    count = 0
    page = 0
    request_count = 0

    follows = []

    # some formatting for logs
    tz = timezone("US/Eastern")
    fmt = "%Y-%m-%d %H:%M:%S %Z%z"

    while True:
        if request_count > 14:  # rate limit stuff
            logger.info(
                f"Pause for rate limit @ {datetime.now(tz).strftime(fmt)} (request_count: {request_count}) - processing page {page} for {uid}"
            )
            time.sleep(60 * 15)  # sad
            request_count = 0  # reset

        try:
            res = request_following(uid, next_token=next_token)
            request_count += 1

        except Exception as e:
            logger.warning(
                f"Exception {e} with next_token {next_token} (request_count: {request_count}) - page {page}: {count} results so far for user_id {uid}"
            )
            raise

        if ("data" not in res.keys()) or ("meta" not in res.keys()):
            break

        count += res["meta"]["result_count"]
        page += 1

        if count == 0:
            break

        follows += res["data"]

        if "next_token" in res["meta"]:
            next_token = res["meta"]["next_token"]
        else:
            break

    return follows, request_count
