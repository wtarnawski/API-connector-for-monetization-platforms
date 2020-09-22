import os
import pickle
import os.path
import pandas as pd
import numpy as np
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from send_mail import send_mail_to_me
# from google_api import summarize_google_play_data
# from appstore_api import summarize_appstore_data

import mopub_oop
import google_oop
import applovin_oop
import smaato_oop
import unityads_oop
import ironsource_oop
import chartboost_oop
import adcolony_oop

for update_function in [
        mopub_oop.update,
        google_oop.update,
        applovin_oop.update,
        smaato_oop.update,
        unityads_oop.update,
        ironsource_oop.update,
        chartboost_oop.update,
        adcolony_oop.update]:

    try:
        update_function()
    except Exception as e:
        send_mail_to_me(
            f'Things gone bad. {e}',
            'Data collection system failure'
        )
        raise Exception

from variables import *


def merge_data(list_of_dataframes):
    df = pd.concat(
        objs=list_of_dataframes,
        ignore_index=True
    )
    df.loc[:,
    YEAR
    ] = pd.DatetimeIndex(df.loc[:, DATE]).year
    df.loc[:,
    MONTH
    ] = pd.DatetimeIndex(df.loc[:, DATE]).month
    aggregated_data = df.groupby(
        [YEAR, MONTH, SOURCE, GAME_NAME, PLATFORM],
        as_index=False
    ).sum()
    return aggregated_data


def push_data_to_drive(merged_data):
    doc_id = "11M0DnIZpf84xbnW93VO7fr4Iak2Q1q6AChGy8tG0_CM"

    # If modifying these scopes, delete the file token.pickle.
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)
    merged_data.fillna('', inplace=True)
    # Call the Sheets API
    values = [merged_data.columns.values.tolist()] \
             + [["Total", "Total", "Total", "Total", "Total", "=SUBTOTAL(9,F3:F)", "=SUBTOTAL(9,G3:G)"]] \
             + merged_data.values.tolist()
    body = {
        'values': values
    }

    from string import ascii_uppercase

    excel_column = {}
    for number, letter in enumerate(ascii_uppercase):
        excel_column[number] = letter

    sheet = service.spreadsheets()
    sheet.values().clear(spreadsheetId=doc_id, range="A1:ZZ1000").execute()
    sheet.values().update(spreadsheetId=doc_id,
                          range=f"Data!A1:{excel_column[len(values[0])]}{len(values) + 1}",
                          valueInputOption="USER_ENTERED",
                          body=body
                          ).execute()
    # values = result.get('values', [])


try:
    storage_client = storage.Client()
    reports_cache_bucket_name = "reports_cache"
    bucket = storage_client.bucket(reports_cache_bucket_name)

    all_reports = [x for x in storage_client.list_blobs(bucket) if 'lifetime' in x.name]
    temp = []
    for x in all_reports:
        # print(x.name)
        report = bucket.blob(x.name).download_as_string()
        temp.append(
            pd.read_csv(
                BytesIO(
                    report
                )
            )
        )
    all_reports_dfs = temp
    push_data_to_drive(
        merged_data=merge_data(
            all_reports_dfs
        )
    )
except Exception as e:
    send_mail_to_me(message=f"Pushing to google spreadsheets failed. {e}", subject='Data collection system failure')
pass
send_mail_to_me(message="Data up to date and sent to the report", subject='Data update finished')
