import pandas as pd

from reservanto import settings
from reservanto.process_data import (convert_nan_to_empty_strings, get_only_once_patients,
                                     get_patients_who_did_not_come_x_days, get_last_visits_from_roihunter,
                                     get_patients_who_did_not_use_their_voucher)
from reservanto.google_sheets import create, export_pandas_df_to_sheets

if __name__ == '__main__':
    title = "reservanto-automatic"
    email_address = settings.GOOGLE_EMAIL_ADDRESS

    df = pd.read_csv(settings.RESERVANTO_DIR / "reservanto.csv", sep=";", encoding="utf-8")

    # choose only relevant columns
    df = df[["title", "createdAt", "start", "end",
             "bookingNote", "customerId", "customer", "customerContact",
             "bookingNoShowState", "hasCustomerNote", "isFreeTime", "noShowStatus"]]

    # drop rows with deleted customers
    df = df[df["customer"] != "Smazáno"]
    # drop duplicates   #TODO
    df.drop_duplicates(keep="last", inplace=True)

    time_columns = ["createdAt", "start", "end"]
    df[time_columns] = df[time_columns].apply(lambda col: pd.to_datetime(col, utc=True))

    # split phone number and email to separate columns
    phone_pattern = r"\+\d{3} (\d{3} \d{3} \d{3})"  # look for + 420 123 456 789; return only 132 456 789
    email_pattern = r"([\w\.-]+@[\w\.-]+\.\w+)"
    df["phoneNumber"] = df["customerContact"].str.extract(phone_pattern)
    df["emailAddress"] = df["customerContact"].str.extract(email_pattern)
    df.drop(columns=["customerContact"], inplace=True)



    # compute info
    x = 100
    a, b, c, d = (get_only_once_patients(df).copy(), get_patients_who_did_not_come_x_days(df, x).copy(),
                  get_last_visits_from_roihunter(df).copy(), get_patients_who_did_not_use_their_voucher(df).copy())
    sheets = {
        "přišli_jen_jednou": a,
        f"nepřišli_{x}_dní": b,
        "poslední_návštěvy_z_roihunteru": c,
        "nevyužité vouchery": d
    }

    # Beautify for export
    for sheet_name in sheets.keys():
        if sheet_name == "nevyužité vouchery":
            sheets[sheet_name] = sheets[sheet_name][["title", "createdAt", "start", "customer",
                                                     "phoneNumber", "emailAddress", "bookingNote",
                                                     "isValidUntil", "isValidVoucher"]]
            sheets[sheet_name]["isValidUntil"] = sheets[sheet_name]["isValidUntil"].dt.strftime("%d. %m. %Y %H:%M")
        else:
            sheets[sheet_name] = sheets[sheet_name][["title", "createdAt", "start", "customer",
                                                     "phoneNumber", "emailAddress", "bookingNote"]]
        # convert datetime to this format: e.g., 05. 03. 2024 18:28
        sheets[sheet_name]["createdAt"] = sheets[sheet_name]["createdAt"].dt.strftime("%d. %m. %Y %H:%M")
        sheets[sheet_name]["start"] = sheets[sheet_name]["start"].dt.strftime("%d. %m. %Y %H:%M")
        sheets[sheet_name] = convert_nan_to_empty_strings(sheets[sheet_name])
        sheets[sheet_name] = sheets[sheet_name][
            sheets[sheet_name]["title"].notna()]  # Get rid of rows with None in title column
        print(sheets[sheet_name].info())

    df = df[["title", "createdAt", "start", "end",
             "customer", "phoneNumber", "emailAddress", "bookingNote",
             "customerId", "bookingNoShowState", "hasCustomerNote", "isFreeTime", "noShowStatus"]]

    df["createdAt"] = df["createdAt"].dt.strftime("%d. %m. %Y %H:%M")
    df["start"] = df["start"].dt.strftime("%d. %m. %Y %H:%M")
    df["end"] = df["end"].dt.strftime("%d. %m. %Y %H:%M")
    df = convert_nan_to_empty_strings(df)
    df = df[df["title"].notna()]  # get rid of rows with None in title column

    # pd.set_option("display.max_columns", None)
    print(df.head())
    print(df.columns)
    print(df.info())

    # export
    spreadsheet_id = create(title, email_address)
    export_pandas_df_to_sheets(spreadsheet_id, df, "všechna_data")
    for key, value in sheets.items():
        export_pandas_df_to_sheets(spreadsheet_id, value, key)

    """
    kolik má zapsaných různých pacientů
    kteří byli jen jednou na vstupním vyšetření - DONE
    kolik hodin denně, týdně, měsíčně máma pracuje
    jsou z roihunteru a kdy byly naposledy - DONE
    kdo tam už xx dní nebyl - DONE
    mají permanentku a nevychodili návštěvy - DONE
    """
