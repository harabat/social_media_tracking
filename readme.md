# Social media tracking

## `queries_list.json`

- lists relevant search keywords and usernames for various competitors (or entities)

## coda_credentials.py

- holds tokens and UUID required to access the Coda REST API

## `competitor_mentions_*.py scripts`

- are run as a cron job
- scrape mentions of competitors listed in `queries_list.json`
- store mentions in a Coda table
- output to a .csv

## `Mentions of competitors and their activity on *.ipynb`

- are run ad hoc
- scrape mentions of competitors listed in `queries_list.json` and the activity of relevant usernames
- store data in a Coda table
