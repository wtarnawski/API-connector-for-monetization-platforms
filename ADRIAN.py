import pandas as pd
from google.cloud import bigquery
"""
Problem: Engagement: Find the distribution of users based on their distinct days of activity (easy)
Day of activity is a day when user posted a comment.
"""

# korzystamy z query zaproponowanego przez Pana, bo zakładamy, że nie zna Pan SQLa.
# Dla ograniczenia ilości pobranych danych wprowadzam modyfikację: bierzemy pod uwagę tylko marzec 2007.
# To wystaczy aby pokazać podejście do rozwiązania tego problemu.
# W finalnym rozwiązaniu należałoby tę linijkę usunąć.

query_str = """
 SELECT author as user_name, time as timestamp 
 FROM `bigquery-public-data.hacker_news.comments`
 WHERE TIMESTAMP_SECONDS(time) BETWEEN '2007-03-01' AND '2007-03-31' 
"""

df = bigquery.Client()\
 .query(query_str)\
 .to_dataframe()

df.loc[:, "timestamp"] = df.loc[:, "timestamp"].apply(
 lambda x: pd.to_datetime(x, unit='s')
)
df.loc[:, "timestamp"] = df.loc[:, "timestamp"].dt.date
df = df.rename(columns={"timestamp": "date"})

days_of_user_activity = df.groupby("user_name").nunique()["date"]
days_of_user_activity=days_of_user_activity.reset_index().rename(columns={"date": "days_of_activity"})

distribution = days_of_user_activity.groupby("days_of_activity").count().reset_index().rename(columns={"user_name": "users"})

distribution.to_csv("data_Wojtek.csv", index=False)