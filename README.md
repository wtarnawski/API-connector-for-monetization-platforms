The code I use to pull and process revenue data from various game monetization platform.

This is a snippet of a data processing system I designed and implemented in my company. 
I pull the data from many sources including Google Cloud Storage and various APIs of monetization platforms.

This script caches the data on the Google Cloud Storage bucket to reduce the API usage.

<h2>Notes</h2>

The reason behind why there is "year" and "month" columns is that the data is pushed to a google spreadsheet later.

This solution:

df.groupby(
    [df["Date"].dt.to_periods(frequency="M"), ...],
    as_index=False
) 


makes it more difficult to fit the data into a spreadsheet as this operation drops the date index and the date columns as well. 
