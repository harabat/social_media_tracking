#!/usr/bin/python3

import csv
from datetime import datetime, timedelta
import json
import pandas as pd
from psaw import PushshiftAPI
import requests
import coda_credentials as coda


def psaw_to_data():
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
        "Activity on Reddit from {} to {}\n".format(
            datetime.fromtimestamp(start), datetime.fromtimestamp(end)
        )
    )

    api = PushshiftAPI()

    data_submissions = pd.DataFrame()
    data_comments = pd.DataFrame()
    competitor_submissions = []
    competitor_comments = []

    for competitor in competitors:
        query = competitor.lower()
        submissions_list = list(
            api.search_submissions(q=query, after=start, before=end)
        )
        submissions = pd.DataFrame([item.d_ for item in submissions_list])
        data_submissions = pd.concat([data_submissions, submissions], ignore_index=True)
        competitor_submissions += [competitor] * len(submissions)

        comments_list = list(api.search_comments(q=query, after=start, before=end))
        comments = pd.DataFrame([item.d_ for item in comments_list])
        data_comments = pd.concat([data_comments, comments], ignore_index=True)
        competitor_comments += [competitor] * len(comments)

        print("{}: {} mentions\n".format(competitor, len(submissions) + len(comments)))

    if not data_comments.empty:
        data_comments[
            ["story_title", "story_score", "story_num_comments"]
        ] = data_comments["permalink"].apply(
            lambda permalink: pd.DataFrame(
                list(
                    api.search_submissions(
                        url="https://www.reddit.com{}".format(
                            permalink[: permalink[:-1].rfind("/") + 1]
                        )
                    )
                )
            ).loc[0, ["title", "score", "num_comments"]]
        )

    data_submissions["competitor"] = competitor_submissions
    data_comments["competitor"] = competitor_comments

    data = {"submissions": data_submissions, "comments": data_comments}

    return data


def reddit_to_coda():
    data = psaw_to_data()

    if data["submissions"].empty and data["comments"].empty:
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
                    "value": "{title}:\n\n{text}".format(
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
                {
                    "column": columns["Content"],
                    "value": 'Comment in "{title}":\n\n{text}'.format(
                        title=data["comments"]["story_title"][row],
                        text=data["comments"]["body"][row],
                    ),
                },
                {"column": columns["Author"], "value": data["comments"]["author"][row]},
                {
                    "column": columns["Metrics"],
                    "value": "Score: {score},\nStory score: {story_score},\nComments: {story_num_comments}".format(
                        score=data["comments"]["score"][row],
                        story_score=data["comments"]["story_score"][row],
                        story_num_comments=data["comments"]["story_num_comments"][row],
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
    print(res)

    # keep a record in a local file
    with open("/home/admin/CAM_backup.csv", "a") as backup:
        record = csv.writer(backup)
        record.writerows([[cell["value"] for cell in row["cells"]] for row in rows])

    return


reddit_to_coda()
