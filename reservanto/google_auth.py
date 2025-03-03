import json
import os

from googleapiclient.discovery import build
from google.oauth2 import service_account

from reservanto import settings

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

credentials = service_account.Credentials.from_service_account_file(settings.RESERVANTO_DIR / "credentials.json",
                                                                    scopes=SCOPES)

spreadsheet_service = build("sheets", "v4", credentials=credentials)
drive_service = build("drive", "v3", credentials=credentials)


