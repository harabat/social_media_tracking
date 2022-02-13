#!/usr/bin/python3

import csv
from datetime import datetime, timedelta
import dotenv
import json
import os
import pandas as pd
import requests


def requests_to_data():
    with open("./queries_list.json", "r") as file:
        competitors_dict = json.loads(file)
    competitors = list(competitors_dict.keys())

    start = int(
        (datetime.now() - timedelta(hours=1))
        .replace(minute=0, second=0, microsecond=0)
        .timestamp()
    )
    end = int(datetime.now().replace(minute=0, second=0, microsecond=0).timestamp())

    print(
        "Activity on HN from {} to {}\n".format(
            datetime.fromtimestamp(start), datetime.fromtimestamp(end)
        )
    )

    data = pd.DataFrame()

    for competitor in competitors:
        query = competitor.lower()
        uri = 'https://hn.algolia.com/api/v1/search_by_date?query="{query}"&numericFilters=created_at_i>{start},created_at_i<{end}&hitsPerPage=1000'.format(
            query=query, start=start, end=end
        )
        res = requests.get(uri).json()["hits"]
        data_query = pd.DataFrame(res)
        data_query["competitor"] = competitor
        data = pd.concat([data, data_query], ignore_index=True)

        print("HN:\n{}: {} mentions\n".format(competitor, len(data_query)))

    return data


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
                        "comment": 'Comment in "{title}":\n\n{text}'.format(
                            title=data["story_title"][row],
                            text=data["comment_text"][row],
                        ),
                    }[data["_tags"][row][0]],
                },
                {"column": columns["Author"], "value": data["author"][row]},
                {
                    "column": columns["Metrics"],
                    "value": "Story score: {points},\nComments: {comments}".format(
                        points=data["points"][row], comments=data["num_comments"][row]
                    )
                    if data["_tags"][row][0] != "comment"
                    else "Story's points: {points},\nStory's comments: {comments}".format(
                        points=requests.get(
                            "https://hn.algolia.com/api/v1/items/"
                            + str(int(data["story_id"][row]))
                        ).json()["points"],
                        comments=len(
                            requests.get(
                                "https://hn.algolia.com/api/v1/items/"
                                + str(int(data["story_id"][row]))
                            ).json()["children"]
                        ),
                    ),
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

    req = requests.post(uri, headers=headers, json=payload)
    req.raise_for_status()
    res = req.json()
    print(res)

    # keep a record in a local file
    with open("/home/admin/CAM_backup.csv", "a") as backup:
        record = csv.writer(backup)
        record.writerows([[cell["value"] for cell in row["cells"]] for row in rows])

    return


hn_to_coda()
