import json

from variables import *
from datetime import datetime, timedelta
from confidential_variables import *


class SmaatoApiConnector(ApiConnectorWithTokenAuthentication):
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


    @staticmethod
    def extract_token_from_auth_response(response):
        return json.loads(response.text)["access_token"]




class SmaatoReport(Report):


    def extract_date(self, x):
        return datetime.strptime(str(x), "[%Y, %m, %d]").strftime(self.api_connector.DATE_FORMAT)

    @staticmethod
    def extract_platform(x):
        return ApplicationType_map[x]

    @staticmethod
    def extract_game_name(x):
        return AppName_map[x]

    @staticmethod
    def extract_revenue(x):
        return x["netRevenue"]

    @staticmethod
    def extract_column_from_criteria(x, contains):
        list_of_dicts = x
        for dict_ in list_of_dicts:
            if dict_["name"] == contains:
                return dict_["value"]
        raise Exception(f"{contains} not found in any dictionary")

    def process_data(self):
        for crit in self.api_connector.dimensions:
            self.new_report.loc[:, crit] = self.new_report.loc[
                                           :,
                                           "criteria"
                                           ].apply(lambda x: self.extract_column_from_criteria(x, crit))
        return super().process_data()

    def compose_params(self, iter_date, end_date):
        params = {
            "criteria": {
                "dimension": self.api_connector.dimensions[0],
                "child": {
                    "dimension": self.api_connector.dimensions[1],
                    "child": {
                        "dimension": self.api_connector.dimensions[2],
                        "child": None
                    },
                },
            },
            "kpi": {
                "grossRevenue": True,
                "netRevenue": True
            },
            "period": {
                "period_type": "fixed",
                "start_date": iter_date.strftime(self.api_connector.DATE_FORMAT),
                "end_date": end_date.strftime(self.api_connector.DATE_FORMAT)
            }
        }

        return params

ApplicationType_map = {
    1: "Mobile Website",
    2: "iOS",
    3: "Android",
    "1": "Mobile Website",
    "2": "iOS",
    "3": "Android"
}
AppName_map = {
    120247264: "Coloren",
    120251127: "Coloren",
    "120247264": "Coloren",
    "120251127": "Coloren",
}
smaato_data = {
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

smaato_data["api_connector"] = SmaatoApiConnector(
    **smaato_data
)

smaatoReport = SmaatoReport(
    **smaato_data
)
def update():
    smaatoReport.pull_from_storage()
    data_freshness = smaatoReport.get_data_freshness()
    if datetime.strptime(data_freshness, "%Y-%m-%d") < datetime.now() - timedelta(days=2):
        smaatoReport.update()
        smaatoReport.push_to_storage()
