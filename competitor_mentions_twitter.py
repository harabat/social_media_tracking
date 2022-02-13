#!/usr/bin/python3

import csv
from datetime import datetime, timedelta
import json
import pandas as pd
import requests
import twint
import coda_credentials as coda


def twint_search(query, start, end):
    c = twint.Config()
    c.Search = query
    c.Since = start
    c.Until = end
    c.Pandas = True
    twint.run.Search(c)
    return twint.storage.panda.Tweets_df


def twint_to_data():
    with open("./queries_list.json", "r") as file:
        competitors_dict = json.loads(file)
    competitors = list(competitors_dict.keys())

    start_datetime = datetime.now() - timedelta(hours=1)
    start = "{date} {hour}:00:00".format(
        date=str(start_datetime).split()[0], hour=str(start_datetime.hour)
    )
    end_date = datetime.now()
    end = "{date} {hour}:00:00".format(
        date=str(end_date).split()[0], hour=str(end_date.hour)
    )

    print("Activity on Twitter from {} to {}\n".format(start, end))

    data = pd.DataFrame()
    competitor_column = []

    for competitor in competitors:
        query = competitor.lower()

        tweets = twint_search(query=query, start=start, end=end)

        data = pd.concat([data, tweets], ignore_index=True)
        competitor_column += [competitor] * len(tweets)

        print("{}: {} mentions\n".format(competitor, len(tweets)))

    data["competitor"] = competitor_column

    return data


def twitter_to_coda():
    data = twint_to_data()

    if data.empty:
        return

    headers = {"Authorization": coda.TOKEN}

    uri = f"https://coda.io/apis/v1/docs/{coda.DOC}/tables/{coda.TABLE}/rows"

    columns = {
        "Competitor": "coda.COL_1",
        "Social media": "coda.COL_2",
        "Subreddit/hashtags": "coda.COL_3",
        "Content": "coda.COL_4",
        "Author": "coda.COL_5",
        "Metrics": "coda.COL_6",
        "Link": "coda.COL_7",
        "Date": "coda.COL_8",
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
                    "value": "Likes: {likes},\nReplies: {replies},\nRetweets: {retweets}".format(
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
    print(res)

    # keep a record in a local file
    with open("/home/admin/CAM_backup.csv", "a") as backup:
        record = csv.writer(backup)
        record.writerows([[cell["value"] for cell in row["cells"]] for row in rows])

    return


twitter_to_coda()
