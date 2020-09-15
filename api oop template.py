import json
from confidential_variables import *

from variables import *
from datetime import datetime, timedelta

# 1. Choose the correct object, either ApiConnector or ApiConnectorWithTokenAuthentication
# and make a class that inherits from it, for example AdColonyApiConnector
class CustomApiConnector(ApiConnectorWithTokenAuthentication):
    # 2. You can adjust the class to you liking.
    def __init__(self, **kwargs):
        self.dimensions = ["Date", "ApplicationId", "ApplicationType"]
        super().__init__(**kwargs)

    def pull_report(self, params, **kwargs):
        self.stats_headers = {
            "Authorization": f"Bearer {self.get_token()}",
            "Content-Type": "application/json",
            "Host": "api.smaato.com"
        }
        self.response = request(
            method=self.method,
            url=self.stats_url,
            json=params,
            headers=self.stats_headers
        )
        if str(self.response.status_code)[0] != "2":
            self._handle_failure()
        if self.data_format == "JSON":
            return pd.read_json(StringIO(self.response.text))
        elif self.data_format == "CSV":
            return pd.read_csv(StringIO(self.response.text))

# 2a. If you chose ApiConnectorWithTokenAuthentication, you should overwrite this method to extract the token value from auth response
    @staticmethod
    def extract_token_from_auth_response(response):
        return json.loads(response.text)["access_token"]



# 3. For example: AdColonyReport
class CustomReport(Report):

# 4. Below you can see four methods for extracting things out of the statistics API response
    def extract_date(self, x):
        return x

    @staticmethod
    def extract_platform(x):
        return x

    @staticmethod
    def extract_game_name(x):
        return x

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

platform_data = {
    "stats_url": r"https://api.smaato.com/v1/reporting/",
    "max_date_span": 99999,
    "data_format": "JSON",

    "api_response_date_string": "Date",
    "api_response_platform_string": "ApplicationType",
    "api_response_app_string": "ApplicationId",
    "api_response_revenue_string": "kpi",

    "api_request_start_date_string": "start_date",
    "api_request_end_date_string": "end_date",
    "api_request_other_parameters_dict": {
        "breakdown": "date,app,platform",
        "metrics": "revenue"
    },
    "method": "POST",

    "auth_request_url": r"https://auth.smaato.com/v2/auth/token/",
    "auth_request_headers": {
        'content-type': 'application/x-www-form-urlencoded'
    },
    "auth_request_data": {
        "client_id": smaato_credentials["client_id"],
        "client_secret": smaato_credentials["client_secret"],
        "grant_type": "client_credentials"
    },
    "token_expiration_time_minutes": 10 * 60,
    "auth_request_method": "POST",
    "cached_token_storage_location": r"smaato/token.json",

    "data_source": "Smaato",
    "lifetime_report_storage_path": r"smaato/smaato_lifetime_report.csv"
}

platform_data["api_connector"] = CustomApiConnector(
    **platform_data
)

customReport = CustomReport(
    **platform_data
)

def update():
    customReport.pull_from_storage()
    data_freshness = customReport.get_data_freshness()
    if datetime.strptime(data_freshness, "%Y-%m-%d") < datetime.now() - timedelta(days=1):
        customReport.update()
        customReport.push_to_storage()
