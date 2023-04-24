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
# # Mentions of competitors and their activity on HackerNews

# %%
from datetime import datetime, timedelta
import dotenv
import json
import os
import pandas as pd
import requests


# %% [markdown]
# ## Requests to data

# %%
def requests_to_data():
    with open('./queries_list.json', 'r') as file:
        competitors_queries = json.loads(file)

    start = int(
        (datetime.now() - timedelta(hours=1))
        .replace(minute=0, second=0, microsecond=0)
        .timestamp()
    )
    end = int(datetime.now().replace(minute=0, second=0, microsecond=0).timestamp())

    print(
        f"Start:    {datetime.fromtimestamp(start)}"
        f"End:      {datetime.fromtimestamp(end)}"
    )

    uri = "https://hn.algolia.com/api/v1/search_by_date?numericFilters=created_at_i>{start},created_at_i<{end}&hitsPerPage=1000".format(
        start=start, end=end
    )

    res = requests.get(uri).json()["hits"]
    data = pd.DataFrame(res)

    data["competitor"] = None

    for row in range(len(data)):
        for col in ["title", "url", "story_text", "comment_text"]:
            try:
                for competitor in competitors_queries:
                    query = competitors_queries[competitor]["search_keyword"]
                    if query in data[col][row]:
                        print(query)
                        data.loc[row, "competitor"] = competitor
                        break

            except TypeError:
                continue

    data = data.loc[data["competitor"].notna()].reset_index(drop=True)

    return data


# %% [markdown]
# ## Coda

# %%
def hn_to_coda():
    dotenv.load_dotenv(dotenv.find_dotenv())

    data = requests_to_data()
    
    if data.empty:
        return

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
                {"column": columns["Social media"], "value": "HackerNews"},
                {
                    "column": columns["Content"],
                    "value": {
                        "story": "{title}:\n{text}".format(
                            title=data["title"][row],
                            text=data["url"][row] or data["story_text"][row],
                        ),
                        "comment": data["comment_text"][row],
                    }[data["_tags"][row][0]],
                },
                {"column": columns["Author"], "value": data["author"][row]},
                {
                    "column": columns["Metrics"],
                    "value": {
                        "story": ":Points {points},\nComments: {comments}".format(
                            points=data["points"][row],
                            comments=data["num_comments"][row],
                        ),
                        "comment": "",
                    }[data["_tags"][row][0]],
                },
                {
                    "column": columns["Link"],
                    "value": "https://news.ycombinator.com/item?id="
                    + data["objectID"][row],
                },
                {
                    "column": columns["Date"],
                    "value": ", ".join(
                        str(datetime.fromtimestamp(data["created_at_i"][row])).split()
                    ),
                },
            ]
        }
        for row in range(len(data))
    ]

    payload = {"rows": rows}
    print(payload)

    req = requests.post(uri, headers=headers, json=payload)
    req.raise_for_status()
    res = req.json()
    print(res)

    return
