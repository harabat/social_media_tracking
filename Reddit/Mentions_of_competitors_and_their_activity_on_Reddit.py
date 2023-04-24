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
# # Mentions of competitors and their activity on Reddit

# %%
from datetime import datetime, timedelta
import dotenv
import json
import os
import pandas as pd
from psaw import PushshiftAPI
import requests


# %% [markdown]
# ## PSAW

# %%
def psaw_to_data():
    with open("./queries_list.json", "r") as file:
        competitors_queries = json.loads(file)

    start = int(
        (datetime.now() - timedelta(hours=1))
        .replace(minute=0, second=0, microsecond=0)
        .timestamp()
    )

    end = int(datetime.now().replace(minute=0, second=0, microsecond=0).timestamp())

    api = PushshiftAPI()

    data_submissions = pd.DataFrame()
    data_comments = pd.DataFrame()
    competitors_submissions = []
    competitors_comments = []

    for competitor in competitors_queries:
        query = competitors_queries[competitor]["search_keyword"]
        print(query)

        submissions_list = list(
            api.search_submissions(q=query, after=start, before=end)
        )
        submissions = pd.DataFrame([item.d_ for item in submissions_list])
        data_submissions = pd.concat([data_submissions, submissions], ignore_index=True)
        competitors_submissions += [competitor] * len(submissions)

        comments_list = list(api.search_comments(q=query, after=start, before=end))
        comments = pd.DataFrame([item.d_ for item in comments_list])
        data_comments = pd.concat([data_comments, comments], ignore_index=True)
        competitors_comments += [competitor] * len(comments)

    data_submissions["competitor"] = competitors_submissions
    data_comments["competitor"] = competitors_comments

    data = {"submissions": data_submissions, "comments": data_comments}

    return data


# %% [markdown]
# ## Coda

# %%
def reddit_to_coda():
    dotenv.load_dotenv(dotenv.find_dotenv())

    data = psaw_to_data()

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
                {
                    "column": columns["Competitor"],
                    "value": data["submissions"]["competitor"][row],
                },
                {"column": columns["Social media"], "value": "Reddit"},
                {
                    "column": columns["Subreddit/hashtags"],
                    "value": data["submissions"]["subreddit"][row],
                },
                {
                    "column": columns["Content"],
                    "value": "{title}:\n{text}".format(
                        title=data["submissions"]["title"][row],
                        text=data["submissions"]["selftext"][row],
                    ),
                },
                {
                    "column": columns["Author"],
                    "value": data["submissions"]["author"][row],
                },
                {
                    "column": columns["Metrics"],
                    "value": "Score: {score},\nComments: {comments}".format(
                        score=data["submissions"]["score"][row],
                        comments=data["submissions"]["num_comments"][row],
                    ),
                },
                {
                    "column": columns["Link"],
                    "value": data["submissions"]["full_link"][row],
                },
                {
                    "column": columns["Date"],
                    "value": ", ".join(
                        str(
                            datetime.fromtimestamp(data["submissions"]["created"][row])
                        ).split()
                    ),
                },
            ]
        }
        for row in range(len(data["submissions"]))
    ]

    rows += [
        {
            "cells": [
                {
                    "column": columns["Competitor"],
                    "value": data["comments"]["competitor"][row],
                },
                {"column": columns["Social media"], "value": "Reddit"},
                {
                    "column": columns["Subreddit/hashtags"],
                    "value": data["comments"]["subreddit"][row],
                },
                {"column": columns["Content"], "value": data["comments"]["body"][row]},
                {"column": columns["Author"], "value": data["comments"]["author"][row]},
                {
                    "column": columns["Metrics"],
                    "value": "Score: {score}".format(
                        score=data["comments"]["score"][row]
                    ),
                },
                {
                    "column": columns["Link"],
                    "value": "https://reddit.com/" + data["comments"]["permalink"][row],
                },
                {
                    "column": columns["Date"],
                    "value": ", ".join(
                        str(
                            datetime.fromtimestamp(data["comments"]["created"][row])
                        ).split()
                    ),
                },
            ]
        }
        for row in range(len(data["comments"]))
    ]

    payload = {"rows": rows}

    req = requests.post(uri, headers=headers, json=payload)
    req.raise_for_status()
    res = req.json()
    res

    return
