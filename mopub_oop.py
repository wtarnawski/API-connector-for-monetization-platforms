from variables import *
from datetime import datetime, timedelta
from confidential_variables import *


# if the report gets fucked up, you can find applovin_historic.csv on my Google Drive
# upload it as the lifetime report to Storage and run this script to update it
# everything should be OK, but if the data you try to download exceeds 90 days, you would get error
#   in case of error, write to AL account manager (currently MK Dilworth) and request the data from them


class MopubApiConnector(ApiConnector):
    def pull_report(self, params, **kwargs):
        del params["delete_me"]
        return super().pull_report(params, **kwargs)


class MopubReport(Report):
    @staticmethod
    def extract_date(x):
        return x

    @staticmethod
    def extract_platform(x):
        if 'ndroi' in x:
            return ANDROID
        if 'iPhone' in x:
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

    def process_data(self):
        self.new_report = self.new_report[self.new_report["Line Item Type"] != "Advanced Bidding Network"]
        return super().process_data()

    def get_last_date(self):
        if len(self.lifetime_report[DATE]) != 0:
            self.last_date = max(self.lifetime_report[DATE])
        else:
            self.last_date = "2020-06-30"
        return self.last_date

mopub_data = {
    "stats_url": r"https://app.mopub.com/reports/custom/api/download_report",
    "max_date_span": 1,
    "data_format": "CSV",

    "api_response_date_string": "Day",
    "api_response_platform_string": "OS",
    "api_response_app_string": "App",
    "api_response_revenue_string": "Revenue",

    "api_request_start_date_string": "date",
    "api_request_end_date_string": "delete_me",  # Mopub accepts only one day requests
    "api_request_other_parameters_dict": {
        "api_key": mopub_credentials["api_key"],
        "report_key": mopub_credentials["report_key"],
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
    "data_source": "MoPub",
    "lifetime_report_storage_path": r"mopub/mopub_lifetime_report.csv",
}

mopub_data["api_connector"] = MopubApiConnector(
    **mopub_data
)

mopubReport = MopubReport(
    **mopub_data
)

def fix_mopub_report():
    from glob import glob
    reports = [pd.read_csv(x) for x in glob(mopub_reports_path + "*.csv")]
    mopubReport.lifetime_report = pd.DataFrame()
    for report in reports:
        mopubReport.new_report = report
        mopubReport.process_data()
        mopubReport.lifetime_report = pd.concat([mopubReport.lifetime_report, mopubReport.new_report])
    mopubReport.update()
    mopubReport.push_to_storage()

# fix_mopub_report()

def update():
    mopubReport.pull_from_storage()
    data_freshness = mopubReport.get_data_freshness()
    data_freshness_datetime = datetime.strptime(data_freshness, "%Y-%m-%d")
    mopub_min_date = datetime.strptime("2020-06-30", "%Y-%m-%d")
    data_freshness_datetime = max(data_freshness_datetime, mopub_min_date)

    if data_freshness_datetime < datetime.now() - timedelta(days=2):
        mopubReport.update()
        mopubReport.push_to_storage()
update()