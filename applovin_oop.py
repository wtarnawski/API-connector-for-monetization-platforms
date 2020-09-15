from variables import *
from datetime import datetime, timedelta
from confidential_variables import *


# if the report gets fucked up, you can find applovin_historic.csv on my Google Drive
# upload it as the lifetime report to Storage and run this script to update it
# everything should be OK, but if the data you try to download exceeds 90 days, you would get error
#   in case of error, write to AL account manager (currently MK Dilworth) and request the data from them


class AppLovinApiConnector(ApiConnector):
    pass


class AppLovinReport(Report):
    @staticmethod
    def extract_date(x):
        return x

    @staticmethod
    def extract_platform(x):
        if 'ndroi' in x:
            return ANDROID
        if 'ios' in x:
            return IOS
        return x

    @staticmethod
    def extract_game_name(x):
        return rename_app(x)

    @staticmethod
    def extract_revenue(x):
        x = round(
            float(
                x[1:] if not isinstance(x, float) else x
            ),
            2
        )
        return x


applovin_data = {
    "stats_url": r"https://r.applovin.com/report",
    "max_date_span": 90,
    "data_format": "CSV",

    "api_response_date_string": "Day",
    "api_response_platform_string": "Platform",
    "api_response_app_string": "Application",
    "api_response_revenue_string": "Revenue",

    "api_request_start_date_string": "start",
    "api_request_end_date_string": "end",
    "api_request_other_parameters_dict": {
        "format": "csv",
        "columns": "day,platform,application,revenue",
        "api_key": applovin_credentials["api_key"],
    },

    # "auth_request_url": r"https://platform.ironsrc.com/partners/publisher/auth",
    # "auth_request_headers": {
    #     "secretkey": ironsource_credentials["Secret Key"],
    #     "refreshToken": ironsource_credentials["Refresh Token"]
    # },
    # "auth_request_data": None,
    # "token_expiration_time_minutes": 60,
    # "auth_request_method": "GET",
    # "cached_token_storage_location": r"ironsource/token.json",

    "method": "GET",
    "data_source": "AppLovin",
    "lifetime_report_storage_path": r"applovin/applovin_lifetime_report.csv",
}

applovin_data["api_connector"] = AppLovinApiConnector(
    **applovin_data
)

appLovinReport = AppLovinReport(
    **applovin_data
)
def update():
    appLovinReport.pull_from_storage()
    data_freshness = appLovinReport.get_data_freshness()
    if datetime.strptime(data_freshness, "%Y-%m-%d") < datetime.now() - timedelta(days=1):
        appLovinReport.update()
        appLovinReport.push_to_storage()
