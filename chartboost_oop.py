from variables import *
from datetime import datetime, timedelta
from confidential_variables import *


class ChartboostApiConnector(ApiConnector):
    pass


class ChartboostReport(Report):

    def extract_date(self, x):
        return x

    @staticmethod
    def extract_platform(x):
        if "oogle" in x or "lay" in x:
            return ANDROID
        if "ios" in x.lower():
            return IOS

    @staticmethod
    def extract_game_name(x):
        return rename_app(x)

    @staticmethod
    def extract_revenue(x):
        return x


chartboost_data = {
    "stats_url": r"https://analytics.chartboost.com/v3/metrics/app",
    "max_date_span": 99999,
    "data_format": "JSON",

    "api_response_date_string": "dt",
    "api_response_platform_string": "platform",
    "api_response_app_string": "app",
    "api_response_revenue_string": "moneyEarned",

    "api_request_start_date_string": "dateMin",
    "api_request_end_date_string": "dateMax",
    "api_request_other_parameters_dict": {
        "aggregate": "daily",
        "role": "publisher",
        **chartboost_credentials
    },
    "method": "GET",

    "data_source": "Chartboost",
    "lifetime_report_storage_path": r"chartboost/chartboost_lifetime_report.csv"
}

chartboost_data["api_connector"] = ChartboostApiConnector(
    **chartboost_data
)

chartboostReport = ChartboostReport(
    **chartboost_data
)
def update():
    chartboostReport.pull_from_storage()
    data_freshness = chartboostReport.get_data_freshness()
    if datetime.strptime(data_freshness, "%Y-%m-%d") < datetime.now() - timedelta(days=2):
        chartboostReport.update()
        chartboostReport.push_to_storage()
