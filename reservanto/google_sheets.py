"""
Look here for the tutorial: https://blog.coupler.io/python-to-google-sheets/
But be cautious, it contains mistakes.
@author Zakhar Yung
"""
import logging

import pandas as pd

from google_auth import spreadsheet_service
from google_auth import drive_service

logger = logging.getLogger(__name__)
logging.basicConfig(encoding="utf-8", level=logging.INFO)


def create(title: str, email_address: str) -> str:
    """
    Create a Google Spreadsheet with the given title and share it with the specified email address.
    If a spreadsheet with the same title already exists, it only returns the existing spreadsheet's ID.

    :param title: The title of the spreadsheet to be created.
    :param email_address: The email address to share the spreadsheet with.
    :return: The ID of the created or existing spreadsheet.
    """
    # Search for an existing spreadsheet with the given title
    query = f"name='{title}' and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
    response = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = response.get('files', [])

    if files:
        spreadsheet_id = files[0]['id']
        logger.info(f"Spreadsheet '{title}' already exists with ID: {spreadsheet_id}")

    else:
        spreadsheet_details = {
            "properties": {"title": title}
        }
        sheet = spreadsheet_service.spreadsheets().create(body=spreadsheet_details, fields="spreadsheetId").execute()
        spreadsheet_id = sheet.get("spreadsheetId")
        logger.info(f"Spreadsheet '{title}' created with ID: {spreadsheet_id}")

        # Set permissions to share the spreadsheet with the specified email address
        permission = {
            "type": "user",
            "role": "writer",
            "emailAddress": email_address
        }
        drive_service.permissions().create(fileId=spreadsheet_id, body=permission).execute()
        logger.info(f"Spreadsheet shared with {email_address}")

    # TODO maybe later: transfer the ownership to the email_address;
    #  the owner is service@whatevergoogle email now
    return spreadsheet_id


def export_pandas_df_to_sheets(spreadsheet_id: str, df: pd.DataFrame, sheet_name: str = "Sheet1") -> None:
    """
    Exports a pandas DataFrame to a specified sheet in a Google Spreadsheet.
    If the sheet does not exist, it will be created.

    Args:
        spreadsheet_id (str): The ID of the target Google Spreadsheet.
        df (pd.DataFrame): The DataFrame containing the data to export.
        sheet_name (str): The name of the sheet within the spreadsheet to export data to.

    Returns:
        None
    """
    # Convert the DataFrame to a list of lists
    values = df.values.tolist()
    columns = df.columns.tolist()
    unpacked_dataframe = [columns] + values

    # Get the current sheets in the spreadsheet
    spreadsheet = spreadsheet_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_titles = [sheet['properties']['title'] for sheet in spreadsheet['sheets']]

    # Check if the sheet exists; if not, create it
    if sheet_name not in sheet_titles:
        add_sheet_request = {
            "addSheet": {
                "properties": {
                    "title": sheet_name
                }}}
        batch_update_request = {"requests": [add_sheet_request]}
        spreadsheet_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body=batch_update_request).execute()

    data = {
        "range": f"{sheet_name}!A1",
        "majorDimension": "ROWS",
        "values": unpacked_dataframe
    }
    body = {
        "valueInputOption": "USER_ENTERED",
        "data": data
    }

    result = spreadsheet_service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body).execute()

    logger.info(f"{result.get('totalUpdatedCells')} cells updated.")


if __name__ == '__main__':
    title = "Python-google-sheets-demo-1"
    email_address = "malyjan3581@gmail.com"

    test_df = pd.DataFrame(
        [[21, 72, 67],
         [23, 78, 69],
         [32, 74, 56],
         [52, 54, 76]],
        columns=['a', 'b', 'c'])

    spreadsheet_id = create(title, email_address)
    export_pandas_df_to_sheets(spreadsheet_id, test_df)
