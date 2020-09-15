import unittest
from variables import *

class TestReport(unittest.TestCase):
    def setUp(self):
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
        report = Report(
            data_source="Some platform",
            lifetime_report_storage_path="testReport.csv",
            api_connector=
        )
    def test_pull_from_storage(self):
        self.assertIn()

    def test_get_last_date(self):
        assert False

    def test_get_data_freshness(self):
        assert False

    def test_push_to_storage(self):
        assert False

    def test_process_data(self):
        assert False

    def test_compose_params(self):
        assert False

    def test_update(self):
        assert False
