from variables import *
from datetime import datetime, timedelta
import json
from confidential_variables import *

class AdColonyApiConnector(ApiConnector):
    DATE_FORMAT = '%m%d%Y'



class AdColonyReport(Report):
    DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    def extract_date(self, x):
        return x

    @staticmethod
    def extract_platform(x):
        if "ndroi" in x.lower():
            return ANDROID
        if "ios" in x.lower():
            return IOS

    @staticmethod
    def extract_game_name(x):
        return rename_app(x)

    @staticmethod
    def extract_revenue(x):
        return x

    def process_data(self):
        self.new_report = pd.read_json(
            "["+
            ",".join([json.dumps(x) for x in pd.read_json(
                adColonyReport.api_connector.response.text).loc[:, "results"]]
            )+
            "]",
            orient="records"
        )
        # If you're wondering what is this spaghetti piece of shit,
        # it's the easiest way I found for processing their spaghetti piece of shit json response.
        # Maybe you can come up with a better idea? Please do :(
        return super().process_data()


adcolony_data = {
    "stats_url": r"http://clients-api.adcolony.com/api/v2/publisher_summary",
    "max_date_span": 90,
    "data_format": "JSON",

    "api_response_date_string": "date",
    "api_response_platform_string": "platform",
    "api_response_app_string": "app_name",
    "api_response_revenue_string": "earnings",

    "api_request_start_date_string": "date",
    "api_request_end_date_string": "end_date",
    "api_request_other_parameters_dict": {
        "user_credentials": adcolony_credentials["user_credentials"],
        "format": "json",
        "date_group": "day",
        "group_by": "app",
    },
    "method": "GET",

    "data_source": "AdColony",
    "lifetime_report_storage_path": r"adcolony/adcolony_lifetime_report.csv"
}

adcolony_data["api_connector"] = AdColonyApiConnector(
    **adcolony_data
)

adColonyReport = AdColonyReport(
    **adcolony_data
)

def update():
    adColonyReport.pull_from_storage()
    data_freshness = adColonyReport.get_data_freshness()
    if datetime.strptime(data_freshness, "%Y-%m-%d") < datetime.now() - timedelta(days=1):
        adColonyReport.update()
        adColonyReport.push_to_storage()

if __name__ == "__main__":
    update()