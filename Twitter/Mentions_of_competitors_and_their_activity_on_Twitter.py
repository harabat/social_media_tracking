# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.5
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Mentions of competitors and their activity on Twitter

# %%
from datetime import datetime, timedelta
import dotenv
import json
import os
import nest_asyncio
import pandas as pd
import requests
import twint

nest_asyncio.apply()


# %% [markdown]
# ## Twint

# %%
def twint_search(query, start, end, query_type):
    c = twint.Config()
    if query_type == "search":
        c.Search = query
    elif query_type == "username":
        c.Username = query
    else:
        raise BaseException("wrong type")
    c.Since = start
    c.Until = end
    c.Pandas = True
    twint.run.Search(c)
    return twint.storage.panda.Tweets_df


# %%
def twint_to_data():
    with open("./queries_list.json", "r") as file:
        competitors_queries = json.loads(file)

    start_datetime = datetime.now() - timedelta(hours=1)
    start = "{date} {hour}:00:00".format(
        date=str(start_datetime).split()[0], hour=str(start_datetime.hour)
    )
    end_date = datetime.now()
    end = "{date} {hour}:00:00".format(
        date=str(end_date).split()[0], hour=str(end_date.hour)
    )

    data = pd.DataFrame()
    competitors = []

    for competitor in competitors_queries:
        query = competitors_queries[competitor]["search_keyword"]
        print(query)
        query_type = "search"

        tweets = twint_search(query=query, start=start, end=end, query_type=query_type)
        data = pd.concat([data, tweets], ignore_index=True)
        competitors += [competitor] * len(tweets)

        if "usernames" in competitors_queries[competitor]:
            username = competitors_queries[competitor]["usernames"]["Twitter"]
            query_type = "username"
            tweets = twint_search(
                query=query, start=start, end=end, query_type=query_type
            )
            data = pd.concat([data, tweets], ignore_index=True)
            competitors += [competitor] * len(tweets)

    data["competitor"] = competitors

    return data


# %% [markdown]
# ## Coda

# %%
def twitter_to_coda():
    dotenv.load_dotenv(dotenv.find_dotenv())

    data = twint_to_data()

    headers = {"Authorization": os.environ["CODA_TOKEN"]}

    uri = f"https://coda.io/apis/v1/docs/{os.environ['CODA_DOC']}/tables/{os.environ['CODA_TABLE']}/rows"

    columns = {
        "Competitor": os.environ["CODA_COL_1"],
        "Social media": os.environ["CODA_COL_2"],
        "Subreddit/hashtags": os.environ["CODA_COL_3"],
        "Content": os.environ["CODA_COL_4"],
        "Author": os.environ["CODA_COL_5"],
        "Metrics": os.environ["CODA_COL_6"],
        "Link": os.environ["CODA_COL_7"],
        "Date": os.environ["CODA_COL_8"],
    }

    rows = [
        {
            "cells": [
                {"column": columns["Competitor"], "value": data["competitor"][row]},
                {"column": columns["Social media"], "value": "Twitter"},
                {
                    "column": columns["Subreddit/hashtags"],
                    "value": ", ".join(data["hashtags"][row]),
                },
                {"column": columns["Content"], "value": data["tweet"][row]},
                {"column": columns["Author"], "value": data["username"][row]},
                {
                    "column": columns["Metrics"],
                    "value": "Likes: {likes},\nReplies: {replies}".format(
                        likes=data["nlikes"][row],
                        replies=data["nreplies"][row],
                        retweets=data["nretweets"][row],
                    ),
                },
                {"column": columns["Link"], "value": data["link"][row]},
                {"column": columns["Date"], "value": data["date"][row]},
            ]
        }
        for row in range(len(data))
    ]

    payload = {"rows": rows}

    req = requests.post(uri, headers=headers, json=payload)
    req.raise_for_status()
    res = req.json()
    res

    return
