import os

from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request

publisher_id = "pub-4415471472607504"

# Authenticate using the client_secrets file.
client_secrets = os.path.join(
    os.path.dirname(__file__), 'credentials_admob.json')
flow = Flow.from_client_secrets_file(
    client_secrets,
    scopes=['https://www.googleapis.com/auth/admob.report'],
    redirect_uri='urn:ietf:wg:oauth:2.0:oob')

# Redirect the user to auth_url on your platform.
auth_url, _ = flow.authorization_url()
print('Please go to this URL: {}\n'.format(auth_url))

# The user will get an authorization code. This code is used to get the
# access token.
code = input('Enter the authorization code: ')
flow.fetch_token(code=code)
credentials = flow.credentials

# Create an AdMob service object on which to run the requests.
admob = build('admob', 'v1', credentials=credentials)

# Get AdMob account information by replacing publisher_id,
# which follows the format "pub-XXXXXXXXXXXXXXXX".
# See https://support.google.com/admob/answer/2784578
# for instructions on how to find your publisher ID.
result = admob.accounts().get(
name='accounts/{}'.format(publisher_id)).execute()

# Set date range.
# AdMob API only supports the account default timezone and "America/Los_Angeles", see
# https://developers.google.com/admob/api/v1/reference/rest/v1/accounts.networkReport/generate
# for more information.
date_range = {
    'startDate': {'year': 2020, 'month': 3, 'day': 1},
    'endDate': {'year': 2020, 'month': 7, 'day': 31}
}

# Set metrics.
metrics = ['ESTIMATED_EARNINGS']

# Set dimensions.
dimensions = ['MONTH', 'APP', 'PLATFORM']

# # Set sort conditions.
# sort_conditions = {'dimension': 'MATCH_RATE', 'order': 'DESCENDING'}

# Set dimension filters.
# dimension_filters = {
#     'dimension': 'COUNTRY',
#     'matchesAny': {
#         'values': ['US', 'CA']
#     }
# }
localizationSettings= {
    'currencyCode': 'USD',
    'languageCode': 'en-US'
  }
# Create network report specifications.
report_spec = {
    'dateRange': date_range,
    'dimensions': dimensions,
    'metrics': metrics,
    'localizationSettings': localizationSettings,
    # 'sortConditions': [sort_conditions],
    # 'dimensionFilters': [dimension_filters]
}

# Create network report request.
request = {'reportSpec': report_spec}

# Execute network report request.
# Get AdMob account information by replacing publisher_id,
# which follows the format "pub-XXXXXXXXXXXXXXXX".
# See https://support.google.com/admob/answer/2784578
# for instructions on how to find your publisher ID.
result = admob.accounts().networkReport().generate(
    parent='accounts/{}'.format(publisher_id), body=request).execute()

# Display results.

agg = {}
for record in result[1:-1]:
    if record["row"]["dimensionValues"]["MONTH"]["value"] not in agg.keys():
        agg[record["row"]["dimensionValues"]["MONTH"]["value"]] = 0
    agg[record["row"]["dimensionValues"]["MONTH"]["value"]] += int(record["row"]["metricValues"]["ESTIMATED_EARNINGS"]["microsValue"])/1000000

print(agg)