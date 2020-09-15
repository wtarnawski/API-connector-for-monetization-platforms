from variables import *
from datetime import datetime, timedelta
from confidential_variables import *


class IronSourceApiConnector(ApiConnectorWithTokenAuthentication):
    def pull_report(self, *args, **kwargs):
        self.stats_headers = {
            "Authorization": f"Bearer {self.get_token()}"
        }
        return super().pull_report(*args, **kwargs)

    @staticmethod
    def extract_token_from_auth_response(response):
        return "".join(response.text.split("\""))


class IronSourceReport(Report):
    @staticmethod
    def extract_date(x):
        return x

    @staticmethod
    def extract_platform(x):
        return x

    @staticmethod
    def extract_game_name(x):
        return rename_app(x.split(" (")[0])

    @staticmethod
    def extract_revenue(x):
        return x[0]['revenue']


ironSource_data = {
    "stats_url": r"https://platform.ironsrc.com/partners/publisher/mediation/applications/v6/stats",
    "max_date_span": 99999, #we're able to download a lifetime report from IS
    "data_format": "JSON",

    "api_response_date_string": "date",
    "api_response_platform_string": "platform",
    "api_response_app_string": "appName",
    "api_response_revenue_string": "data",

    "api_request_start_date_string": "startDate",
    "api_request_end_date_string": "endDate",
    "api_request_other_parameters_dict": {
        "breakdown": "date,app,platform",
        "metrics": "revenue"
    },

    "auth_request_url": r"https://platform.ironsrc.com/partners/publisher/auth",
    "auth_request_headers": {
        "secretkey": ironsource_credentials["Secret Key"],
        "refreshToken": ironsource_credentials["Refresh Token"]
    },
    "auth_request_data": None,
    "token_expiration_time_minutes": 60,
    "auth_request_method": "GET",
    "cached_token_storage_location": r"ironsource/token.json",

    "method": "GET",
    "data_source": "IronSource",
    "lifetime_report_storage_path": r"ironsource/ironsource_lifetime_report.csv",
}

ironSource_data["api_connector"] = IronSourceApiConnector(
    **ironSource_data
)

ironSourceReport = IronSourceReport(
    **ironSource_data
)
def update():
    ironSourceReport.pull_from_storage()
    data_freshness = ironSourceReport.get_data_freshness()
    if datetime.strptime(data_freshness, "%Y-%m-%d") < datetime.now() - timedelta(days=2):
        ironSourceReport.update()
        ironSourceReport.push_to_storage()
