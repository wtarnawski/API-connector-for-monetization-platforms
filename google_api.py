from __future__ import print_function
import glob
from datetime import datetime, timedelta, date
from zipfile import ZipFile
import pandas as pd
from variables import *
from google.cloud import storage
from confidential_variables import *
bucket_name = "pubsite_prod_rev_03934830500243960509"
source_blob_name = "earnings/earnings_201703_8268780346132393-20.zip"

storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)


def download_missing_reports_google_play(storage_client, bucket):
    for blob in storage_client.list_blobs(bucket,
                                          prefix='earnings'):
        try:
            with open(destination_path + blob.name.replace(r"/", "\\")) as csvfile:
                pass
        except FileNotFoundError:
            bucket.blob(blob.name).download_to_filename(destination_path + blob.name.replace(r"/", "\\"))


def extract_all_zipfiles_in_directory(path):
    files_list = glob.glob(path + "*.zip")
    for file in files_list:
        with ZipFile(file, 'r') as zipObj:
            # Extract all the contents of zip file in current directory
            zipObj.extractall(path=google_reports_path + "files\\")


class Report:
    def __init__(self, filename):
        self.year = int(
            filename.split("_")[1][:4]
        )
        self.month = int(
            filename.split("_")[1][4:6]
        )
        self._dataframe = pd.read_csv(
            filename
        )

    def as_dataframe(self):
        return self._dataframe


def load_csvs_into_df(from_date):
    merged_table = pd.DataFrame()
    files_list = glob.glob(csv_files_location + "*.csv")

    for file in files_list:
        report = Report(
            filename=file
        )
        if report.year < from_date:
            continue
        merged_table = pd.concat([merged_table, report.as_dataframe()])
    return merged_table


def summarize_google_play_data():
    download_missing_reports_google_play(storage_client, bucket)
    extract_all_zipfiles_in_directory(google_reports_path)
    merged_table = load_csvs_into_df(from_date=2017)
    merged_table.reset_index(
        inplace=True,
        drop=True
    )
    df = merged_table[
        ['Transaction Date', 'Product id', 'Amount (Merchant Currency)']
    ]
    df = df.fillna("")
    df.loc[:, "Datetime column"] = df.loc[:, "Transaction Date"].map(lambda x: datetime.strptime(x, "%b %d, %Y"))
    df.loc[:, "Game name"] = df.loc[:, "Product id"].map(
        lambda x: x.split(".")[-1]
    )
    df.rename(columns={'Amount (Merchant Currency)': "Revenue in PLN"}, inplace=True)
    df = df[
        ["Datetime column", "Game name", "Revenue in PLN"]
    ]

    df.reset_index(inplace=True, drop=True)
    df.loc[:, "Source"] = "Google Play"

    df.loc[:, "Game name"] = df.loc[:, "Game name"].apply(lambda x: rename_app(x))
    df.loc[:, "year"] = df.loc[:, "Datetime column"].dt.year
    df.loc[:, "month"] = df.loc[:, "Datetime column"].dt.month
    df.drop(
        columns=["Datetime column"],
        inplace=True
    )
    aggregated_data = df.groupby(
        ["year", "month", "Source", "Game name"],
        as_index=False
    ).sum()
    aggregated_data.loc[:, "platform"] = "Android"
    return aggregated_data
df = summarize_google_play_data()
print()