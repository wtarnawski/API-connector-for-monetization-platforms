from variables import *
import pandas as pd
from datetime import datetime, timedelta
from confidential_variables import *


class UnityApiConnector(ApiConnector):
    def pull_report(self, **kwargs):
        self.stats_headers = {
            "Accept": "text / csv"
        }
        return super().pull_report(**kwargs)


class UnityAdsReport(Report):
    @staticmethod
    def extract_date(x):
        return datetime.date(pd.to_datetime(x))

    @staticmethod
    def extract_platform(x):
        if 'ndro' in x.lower():
            return ANDROID
        if 'ios' in x.lower():
            return IOS

    @staticmethod
    def extract_game_name(x):
        return rename_app(x)

    @staticmethod
    def extract_revenue(x):
        return x


unity_data = {
    "stats_url": r"https://monetization.api.unity.com/stats/v1/operate/organizations/369682",
    "max_date_span": 365,
    "data_format": "CSV",

    "api_response_date_string": "timestamp",
    "api_response_platform_string": "platform",
    "api_response_app_string": "source_name",
    "api_response_revenue_string": "revenue_sum",

    "api_request_start_date_string": "start",
    "api_request_end_date_string": "end",
    "api_request_other_parameters_dict": {
        "scale": "day",
        "fields": "revenue_sum",
        "groupBy": "platform,game",
        "apikey": unityads_credentials['api_key']
    },

    "method": "GET",
    "data_source": "UnityAds",
    "lifetime_report_storage_path": "unityads/unityads_lifetime_report.csv",
}

unity_data["api_connector"] = UnityApiConnector(
    **unity_data
)

unityReport = UnityAdsReport(
    **unity_data
)
def update():
    unityReport.pull_from_storage()
    data_freshness = unityReport.get_data_freshness()
    if datetime.strptime(data_freshness, "%Y-%m-%d") < datetime.now() - timedelta(days=2):
        unityReport.update()
        unityReport.push_to_storage()
