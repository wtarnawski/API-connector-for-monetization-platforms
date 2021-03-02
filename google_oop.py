from __future__ import print_function
import glob
from datetime import datetime, timedelta, date
from zipfile import ZipFile
import pandas as pd
from variables import *
from google.cloud import storage
from confidential_variables import *
import json

storage_client = storage.Client()
bucket = storage_client.bucket(google_play_bucket_name)

class GooglePlayReport(Report):
    @staticmethod
    def read_df_from_gp_blob(blob):
        with ZipFile(BytesIO(blob), 'r') as obj:
            temp_extracted = obj.read(obj.filelist[0].filename)
        df = pd.read_csv(BytesIO(temp_extracted))
        return df
# 4. Below you can see four methods for extracting things out of the statistics API response
    def extract_date(self, x):
        return datetime.strptime(x, "%b %d, %Y").strftime(self.DATE_FORMAT)

    @staticmethod
    def extract_platform(x):
        return ANDROID

    @staticmethod
    def extract_game_name(x):
        return rename_app(x.split(".")[-1])

    @staticmethod
    def extract_revenue(x):
        return x

    # def process_data(self):
    #     for crit in self.api_connector.dimensions:
    #         self.new_report.loc[:, crit] = self.new_report.loc[
    #                                        :,
    #                                        "criteria"
    #                                        ].apply(lambda x: self.extract_column_from_criteria(x, crit))
    #     return super().process_data()

    def compose_params(self, iter_date, end_date):
        super().compose_params(iter_date, end_date)

    def get_last_date(self):
        if len(self.lifetime_report[DATE]) != 0:
            self.last_date = max(self.lifetime_report[DATE])
        else:
            self.last_date = "2015-08-01"
        return self.last_date
    @staticmethod
    def extract_date_from_blob_name(blob):
        # like 'PlayApps_201508.csv'
        return blob.name.split(".")[0].split("_")[1]
    def pull_publisher_revenue(self, date):
        temp = None
        date = date.strftime("%Y%m")
        blobs_list = storage_client.list_blobs(bucket, prefix='earnings')
        for blob in blobs_list:
            if date in self.extract_date_from_blob_name(blob):
                print(date)
                temp = bucket.blob(blob.name).download_as_string()
                break
        if temp is None:
            raise Exception("Couldn't find a blob for this date")
        self.new_report = self.read_df_from_gp_blob(temp)
        taxes = self.new_report["Transaction Type"] == "Tax"
        positive_revenue = (self.new_report['Amount (Merchant Currency)'] > 0)
        both_tax_and_positive_revenue = taxes & positive_revenue
        if both_tax_and_positive_revenue.any():
            print()
        return self.new_report
    def update(self):
        # the point of this function is to iteratively update self.lifetime_report to include new days
        if self.last_date is None:
            self.get_last_date()
        day_next_to_the_last = (datetime.strptime(self.last_date, self.DATE_FORMAT) + timedelta(days=1)).date()
        day_before_today = (datetime.now() - timedelta(days=2)).date()

        iter_date = day_next_to_the_last
        # this shouldn't happen as we're always checking
        #   the data freshness and for that we need to pull the report anyway
        if self.lifetime_report is None:
            self.pull_from_storage()

        print(self.data_source)
        while iter_date <= datetime.now().date().replace(day=1):
            print(iter_date)
            end_date = min(iter_date + self.api_connector.max_date_span, day_before_today)
            # params is always like {start: ..., end:..., dimension_breakdown:..., metrics:...}
            iter_date += self.api_connector.max_date_span
            if iter_date.year == datetime.now().year and iter_date.month >= datetime.now().month:
                break
            self.new_report = self.pull_publisher_revenue(date=iter_date)
            try:
                self.new_report = self.process_data(revenue_currency=REVENUE_IN_PLN)
            except EmptyResponseReturned:
                continue
            self.lifetime_report = pd.concat([self.lifetime_report, self.new_report])

        self.lifetime_report.loc[:, DATA_FRESHNESS] = datetime.now().date()

google_play_data = {
    "max_date_span": 30.43,
    "data_format": "CSV",

    "api_response_date_string": 'Transaction Date',
    "api_response_platform_string": 'Tax Type', #this is not a mistake, we just have to provide any column as we're overwtriing it later anyway
    "api_response_app_string": "Product id",
    "api_response_revenue_string": 'Amount (Merchant Currency)',

    "data_source": "Google Play",
    "lifetime_report_storage_path": r"google_play/google_play_lifetime_report.csv",

# below are not used, as we use the google cloud API for pulling the reports
    "stats_url": r"asd",
    "api_request_start_date_string": "start_date",
    "api_request_end_date_string": "end_date",
    "api_request_other_parameters_dict": {
        "breakdown": "date,app,platform",
        "metrics": "revenue"
    },
    "method": "GET",


}

google_play_data["api_connector"] = ApiConnector(
    **google_play_data
)

customReport = GooglePlayReport(
    **google_play_data
)

def update():
    customReport.pull_from_storage()
    data_freshness = customReport.get_data_freshness()
    if datetime.strptime(data_freshness, "%Y-%m-%d") < datetime.now() - timedelta(days=1):
        customReport.update()
        customReport.push_to_storage()