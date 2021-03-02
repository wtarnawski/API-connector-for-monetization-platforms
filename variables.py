from __future__ import print_function

import os
from datetime import datetime, timedelta
from io import BytesIO, StringIO
from google.cloud import storage
from requests import request
from google.cloud.exceptions import NotFound
import pandas as pd
import math
from send_mail import send_mail_to_me


YEAR = "year"
MONTH = "month"
REVENUE_IN_USD = "Revenue in USD"
REVENUE_IN_PLN = "Revenue in PLN"
PLATFORM = "Platform"
GAME_NAME = "Game name"
SOURCE = "Source"
ANDROID = "Android"
IOS = "iOS"
DATE = "date"
TOKEN_EXPIRATION_DATE = "expiration_date"
DATA_FRESHNESS = "Data freshness"

storage_client = storage.Client()
reports_cache_bucket_name = "reports_cache"
bucket = storage_client.bucket(reports_cache_bucket_name)


class ApiError(Exception):
    pass


class ApiConnector:
    DATE_FORMAT = "%Y-%m-%d"

    def __init__(self,
                 stats_url,
                 max_date_span,
                 data_format,
                 api_response_date_string,
                 api_response_platform_string,
                 api_response_app_string,
                 api_response_revenue_string,
                 api_request_start_date_string,
                 api_request_end_date_string,
                 api_request_other_parameters_dict,
                 method="GET",
                 **kwargs):
        self.stats_url = stats_url
        self.method = method
        self.max_date_span = timedelta(days=max_date_span)
        self.stats_headers = None
        if data_format not in ("JSON", "CSV"):
            raise ValueError('data_format parameter should be "JSON" or "CSV"')
        self.data_format = data_format
        self.api_response_date_string = api_response_date_string
        self.api_response_platform_string = api_response_platform_string
        self.api_response_app_string = api_response_app_string
        self.api_response_revenue_string = api_response_revenue_string
        self.api_request_start_date_string = api_request_start_date_string
        self.api_request_end_date_string = api_request_end_date_string
        self.api_request_other_parameters_dict = api_request_other_parameters_dict
        self.response = None

    def _handle_failure(self):
        if self.response:
            raise ApiError(f"Request returned: {self.response.status_code}: {self.response.text}")
        else:
            raise ApiError(f"No response was received.")

    def pull_report(self, params, **kwargs):
        self.response = request(
            method=self.method,
            url=self.stats_url,
            params=params,
            headers=self.stats_headers
        )
        if str(self.response.status_code)[0] != "2":
            self._handle_failure()
        if self.data_format == "JSON":
            return pd.read_json(StringIO(self.response.text))
        elif self.data_format == "CSV":
            if "mopub" in self.stats_url:
                tfile= r"C:\appstore\temp"
                try:
                    os.remove(tfile)
                except FileNotFoundError:
                    pass

                with open(tfile, "a") as tempwrite:

                    tempwrite.write(self.response.text.encode('ascii', 'ignore').decode("utf-8"))
                cols = [
                    self.api_response_app_string,
                    self.api_response_date_string,
                    self.api_response_platform_string,
                    self.api_response_revenue_string,
                    'Adgroup Type'
                ]
                df = pd.read_csv(tfile, usecols=cols)
                return df
            return pd.read_csv(StringIO(self.response.text))


class TokenExpiredError(Exception):
    """Get new auth token, this one has gone bad"""
    pass


class ApiConnectorWithTokenAuthentication(ApiConnector):

    def __init__(self,
                 auth_request_url,
                 auth_request_data,
                 auth_request_headers,
                 token_expiration_time_minutes,
                 auth_request_method,
                 cached_token_storage_location,
                 *args, **kwargs
                 ):
        self.auth_request_url = auth_request_url
        self.auth_request_data = auth_request_data
        self.auth_request_headers = auth_request_headers
        self.token_expiration_time_minutes = token_expiration_time_minutes
        self.auth_request_method = auth_request_method
        self.token = None
        self.cached_token_storage_location = cached_token_storage_location
        super().__init__(*args, **kwargs)

    def __get_new_token(self):
        self.auth_response = request(
            method=self.auth_request_method,
            url=self.auth_request_url,
            data=self.auth_request_data,
            headers=self.auth_request_headers
        )
        if str(self.auth_response.status_code)[0] != "2":
            self._handle_failure()
        self.token = {
            "token": self.extract_token_from_auth_response(self.auth_response),
            TOKEN_EXPIRATION_DATE: (
                    datetime.now() + timedelta(minutes=self.token_expiration_time_minutes - 2)).isoformat()
        }
        self.__cache_new_token()

    @staticmethod
    def extract_token_from_auth_response(response):
        # this varies across platforms
        raise NotImplementedError

    def __cache_new_token(self):

        blob = bucket.blob(self.cached_token_storage_location)
        try:
            blob.upload_from_string(str(self.token))
        except Exception:
            self._handle_failure()

    def __get_cached_token(self):
        bucket_name = reports_cache_bucket_name
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(self.cached_token_storage_location)

        self.token = eval(blob.download_as_string())
        if datetime.fromisoformat(self.token[TOKEN_EXPIRATION_DATE]) < datetime.now():
            raise TokenExpiredError

    def get_token(self):
        try:
            self.__get_cached_token()
        except (NotFound, TokenExpiredError):
            self.__get_new_token()

        return self.token['token']


class DataFreshnessError(Exception):
    pass


class EmptyResponseReturned(Exception):
    """
    When pulled the data from the API no records were returned.
    This should be fired when attempting to process the data, but no data is present in self.new_report
    """
    pass


class Report:
    DATE_FORMAT = "%Y-%m-%d"

    def __init__(self, data_source, lifetime_report_storage_path, api_connector, **kwargs):
        self.data_source = data_source
        self.lifetime_report_storage_path = lifetime_report_storage_path
        self.lifetime_report = None
        self.last_date = None
        self.new_report = None
        self.api_connector = api_connector
        self.data_freshness = None

    def __repr__(self):
        return self.data_source
    def __str__(self):
        return self.data_source

    def pull_from_storage(self):
        blob = bucket.blob(self.lifetime_report_storage_path)
        try:
            report_string = blob.download_as_string()
            self.lifetime_report = pd.read_csv(BytesIO(report_string))
            self.get_last_date()  # stored in self.last_date
        except NotFound:
            self.lifetime_report = pd.DataFrame(
                columns=[DATE, PLATFORM, GAME_NAME, REVENUE_IN_USD, REVENUE_IN_PLN, DATA_FRESHNESS]
            )
        return self.lifetime_report

    def get_last_date(self):
        if len(self.lifetime_report[DATE]) != 0:
            self.last_date = max(self.lifetime_report[DATE])
        else:
            self.last_date = "2016-12-31"
        return self.last_date

    def get_data_freshness(self):
        try:
            if DATA_FRESHNESS not in self.lifetime_report.columns:
                raise DataFreshnessError

            if len(self.lifetime_report[DATA_FRESHNESS]) == 0:
                raise DataFreshnessError

            max_value = max(self.lifetime_report[DATA_FRESHNESS])
            try:
                if math.isnan(max_value):
                    raise DataFreshnessError
            except TypeError:  # you can't perform isnan on a str, only float; but if it's string, then it contains the '%Y-%m-%d' and that's OK
                pass

            self.data_freshness = max_value
        except DataFreshnessError:
            self.data_freshness = "2016-12-31"
        return self.data_freshness

    def push_to_storage(self):
        blob = bucket.blob(self.lifetime_report_storage_path)
        self.lifetime_report.loc[:, SOURCE] = self.data_source
        blob.upload_from_string(self.lifetime_report.to_csv(index=False))

    @staticmethod
    def extract_date(x):
        raise NotImplementedError

    @staticmethod
    def extract_platform(x):
        raise NotImplementedError

    @staticmethod
    def extract_game_name(x):
        raise NotImplementedError

    @staticmethod
    def extract_revenue(x):
        raise NotImplementedError

    def process_data(self, revenue_currency=REVENUE_IN_USD):

        date_string = self.api_connector.api_response_date_string
        platform_string = self.api_connector.api_response_platform_string
        game_name_string = self.api_connector.api_response_app_string
        revenue_string = self.api_connector.api_response_revenue_string
        if self.new_report.size == 0:
            raise EmptyResponseReturned

        if self.data_source == "Google Play":
            # Eliminate nans from the Google Play older reports
            self.new_report.loc[
                (self.new_report["Transaction Type"] == "Tax") & (self.new_report["Product id"].isna()),
                "Product id"
            ] = "Tax"

        df = self.new_report.loc[:,
             [date_string,
              platform_string,
              game_name_string,
              revenue_string]
             ]

        df.loc[
        :,
        revenue_currency
        ] = df.loc[
            :,
            revenue_string
            ].apply(self.extract_revenue)

        if self.data_source != "Google Play":
            df = df[
                df[revenue_currency] > 0
                ]

        df.loc[
        :,
        DATE
        ] = df.loc[
            :,
            date_string
            ].apply(self.extract_date)

        df.loc[
        :,
        PLATFORM
        ] = df.loc[
            :,
            platform_string
            ].apply(self.extract_platform)

        df.loc[
        :,
        GAME_NAME
        ] = df.loc[
            :,
            game_name_string
            ].apply(self.extract_game_name)

        self.new_report = df.groupby(
            [DATE, PLATFORM, GAME_NAME],
            as_index=False
        ).sum().loc[:, [DATE, PLATFORM, GAME_NAME, revenue_currency]]
        return self.new_report

    def compose_params(self, iter_date, end_date):
        return {
            self.api_connector.api_request_start_date_string:
                iter_date.strftime(self.api_connector.DATE_FORMAT),
            self.api_connector.api_request_end_date_string:
                end_date.strftime(self.api_connector.DATE_FORMAT),
            **self.api_connector.api_request_other_parameters_dict
        }

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
        while iter_date <= day_before_today:
            print(iter_date)
            end_date = min(iter_date + self.api_connector.max_date_span, day_before_today)
            # params is always like {start: ..., end:..., dimension_breakdown:..., metrics:...}
            #   but we build this from the object itself
            params = self.compose_params(iter_date, end_date)
            iter_date += self.api_connector.max_date_span
            self.new_report = self.api_connector.pull_report(
                params=params
            )
            try:
                self.new_report = self.process_data()
            except EmptyResponseReturned:
                continue
            self.new_report = self.new_report.groupby(
                [DATE, PLATFORM, GAME_NAME],
                as_index=False
            ).sum()
            self.lifetime_report = pd.concat([self.lifetime_report, self.new_report])
            self.new_report = None

        self.lifetime_report.loc[:, DATA_FRESHNESS] = datetime.now().date()
