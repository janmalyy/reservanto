import datetime

import pandas as pd

from reservanto import settings
from reservanto.process_data import (convert_nan_to_empty_strings, get_only_once_patients,
                                     get_patients_who_did_not_come_x_days, get_last_visits_from_roihunter,
                                     get_patients_who_did_not_use_their_voucher, get_visits_from_roihunter_for_month)
from reservanto.google_sheets import create, export_pandas_df_to_sheets


if __name__ == '__main__':
    title = "reservanto-automatic-" + str(datetime.date.today())
    # title = "reservanto-automatic_4.3.2025"
    email_address = settings.GOOGLE_EMAIL_ADDRESS
    spreadsheet_id = create(title, email_address)
    # change before export for the actual csv file!
    # df = pd.read_csv(settings.RESERVANTO_DIR / "reservanto" / "reservanto_2025-04-06.csv", sep=";", encoding="utf-8")

    # choose only relevant columns
    df = df[["title", "createdAt", "start", "end",
             "bookingNote", "customerId", "customer", "customerContact",
             "bookingNoShowState", "hasCustomerNote", "isFreeTime", "noShowStatus"]]

    # drop rows with deleted customers
    df = df[df["customer"] != "Smazáno"]
    # drop duplicates
    df.drop_duplicates(keep="last", inplace=True)

    time_columns = ["createdAt", "start", "end"]
    df[time_columns] = df[time_columns].apply(lambda col: pd.to_datetime(col, utc=True))

    # split phone number and email to separate columns
    phone_pattern = r"\+\d{3} (\d{3} \d{3} \d{3})"  # look for + 420 123 456 789; return only 132 456 789
    email_pattern = r"([\w\.-]+@[\w\.-]+\.\w+)"
    df["phoneNumber"] = ""
    df["emailAddress"] = ""
    phone_extracted = df["customerContact"].str.extract(phone_pattern)
    email_extracted = df["customerContact"].str.extract(email_pattern)
    # update the values only where extraction succeeded
    df.loc[phone_extracted.notna().any(axis=1), "phoneNumber"] = phone_extracted[0]
    df.loc[email_extracted.notna().any(axis=1), "emailAddress"] = email_extracted[0]
    df.drop(columns=["customerContact"], inplace=True)

    # compute info
    x = 100
    a, b, c, d, e = (get_only_once_patients(df.copy()), get_patients_who_did_not_come_x_days(df.copy(), x),
                     get_last_visits_from_roihunter(df.copy()), get_visits_from_roihunter_for_month(df.copy(), 3),
                     get_patients_who_did_not_use_their_voucher(df.copy()))
    sheets = {
        "přišli_jen_jednou": a,
        f"nepřišli_{x}_dní": b,
        "poslední_návštěvy_z_roihunteru": c,
        "návštěvy_z_roihunteru_za_březen": d,
        "nevyužité_vouchery": e,

    }

    # Beautify for export
    for sheet_name in sheets.keys():
        if sheet_name == "nevyužité_vouchery":
            sheets[sheet_name] = sheets[sheet_name][["title", "createdAt", "start", "customer",
                                                     "phoneNumber", "emailAddress", "bookingNote",
                                                     "isValidUntil", "isValidVoucher"]]
            sheets[sheet_name]["isValidUntil"] = sheets[sheet_name]["isValidUntil"].dt.strftime(date_format)
        else:
            sheets[sheet_name] = sheets[sheet_name][["title", "createdAt", "start", "customer",
                                                     "phoneNumber", "emailAddress", "bookingNote"]]
        # convert datetime to this format: e.g., 05.03.2024 18:28
        date_format = "'%d.%m.%Y %H:%M"
        sheets[sheet_name]["createdAt"] = sheets[sheet_name]["createdAt"].dt.strftime(date_format)
        sheets[sheet_name]["start"] = sheets[sheet_name]["start"].dt.strftime(date_format)
        sheets[sheet_name] = convert_nan_to_empty_strings(sheets[sheet_name])
        sheets[sheet_name] = sheets[sheet_name][
            sheets[sheet_name]["title"].notna()]  # Get rid of rows with None in title column
        # print(sheets[sheet_name].info())

    df = df[["title", "createdAt", "start", "end",
             "customer", "phoneNumber", "emailAddress", "bookingNote",
             "customerId", "bookingNoShowState", "hasCustomerNote", "isFreeTime", "noShowStatus"]]

    df["createdAt"] = df["createdAt"].dt.strftime(date_format)
    df["start"] = df["start"].dt.strftime(date_format)
    df["end"] = df["end"].dt.strftime(date_format)
    df = convert_nan_to_empty_strings(df)
    df = df[df["title"].notna()]  # get rid of rows with None in title column

    # pd.set_option("display.max_columns", None)
    print(df.head())
    print(df.columns)
    print(df.info())

    # export
    export_pandas_df_to_sheets(spreadsheet_id, df, "všechna_data")
    for key, value in sheets.items():
        export_pandas_df_to_sheets(spreadsheet_id, value, key)
