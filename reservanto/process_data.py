"""
We group patients based on column `customer` (their name and surname) as there is a mess in input data and same persons
have multiple IDs.
"""

import datetime
import re
import time
from datetime import timedelta

import numpy as np
import pandas as pd
from pandas._libs import NaTType

from reservanto import settings


def convert_date_to_unix_timestamp(year: int, month: int, day: int) -> float:
    date_time = datetime.datetime(year, month, day)
    return time.mktime(date_time.timetuple())


def convert_nan_to_empty_strings(df: pd.DataFrame) -> pd.DataFrame:
    # this has to be done before converting to json
    df = df.replace(np.nan, None)  # replace NaN with None
    df = df.replace("", None)  # replace "" with None
    df = df.replace("nan", None)
    return df


def get_only_once_patients(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get patients who came only for the initial examination and never more
    """
    value_counts = df['customer'].value_counts()
    only_once_patients_ids = value_counts[value_counts == 1].index
    filtered_df = df[(df['title'] == 'Vstupní vyšetření') & (df['customer'].isin(only_once_patients_ids))]
    return filtered_df


def get_last_visits(df: pd.DataFrame) -> pd.DataFrame:
    last_visits = df.loc[df.groupby("customer")["start"].idxmax()]  # magic is happening here
    return last_visits.sort_index()


def get_patients_who_did_not_come_x_days(df: pd.DataFrame, x: int = 30) -> pd.DataFrame:
    """
    Returns a DataFrame containing only the last visits of patients
    who have not visited in the last `x` days.

    :param df: DataFrame containing at least 'customer' and 'start' columns.
               The 'start' column should be in datetime format with timezone info.
    :param x: Number of days to check for absence.
    :return: Filtered DataFrame with patients who haven't visited in `x` days.
    """
    today = pd.Timestamp.utcnow()  # Use timezone-aware UTC timestamp
    threshold_date = today - pd.Timedelta(days=x)

    last_visits = get_last_visits(df)

    return last_visits[last_visits["start"] < threshold_date]


roi_pattern = r"[rR][oO][iI]"


def get_last_visits_from_roihunter(df: pd.DataFrame) -> pd.DataFrame:
    last_visits = get_last_visits(df)
    last_visits["bookingNote"] = last_visits["bookingNote"].fillna("")
    last_visits["emailAddress"] = last_visits["emailAddress"].fillna("")
    return last_visits[last_visits["bookingNote"].str.contains(roi_pattern)
                       | last_visits["emailAddress"].str.contains(roi_pattern)]


def get_visits_from_roihunter_for_month(df: pd.DataFrame, month, year=2025) -> pd.DataFrame:
    return df[(df["bookingNote"].str.contains(roi_pattern) | df["emailAddress"].str.contains(roi_pattern))
              & (df["start"] >= pd.Timestamp(year=2025, month=month, day=1, tz="UTC"))
              & (df["start"] < pd.Timestamp(year=2025, month=month+1, day=1, tz="UTC"))
              ]


# ----------------------------------------------------------------------
# voucher functions
# ----------------------------------------------------------------------

pattern = r"(\d+)[\/\\](\d+)"  # match e.g. 3/4, 1\3, 10/12


def get_validity(date: datetime.datetime, total_visits: int) -> tuple[bool, timedelta | NaTType]:
    """
    :param date: when the voucher became valid
    :param total_visits: should be 3 or 5 or 10; determines how long the voucher is valid
    :return: Return whether the voucher is still valid or not and until when it is valid
    For simplicity, the month has always 31 days.
    """
    if total_visits == 3:
        months = 4
    elif total_visits == 5:
        months = 8
    elif total_visits == 10:
        months = 12
    else:
        raise ValueError(f"The voucher should be either for 3 or for 5 visits, {total_visits} came instead.")
    today = pd.Timestamp.utcnow()
    valid_days = months * 31
    threshold_date = today - pd.Timedelta(days=valid_days)
    return date > threshold_date, date + pd.Timedelta(days=valid_days)


def get_visits(booking_note: str):
    match = re.match(pattern, booking_note)
    if match:
        current_visit = int(match[1])
        total_visits = int(match[2])
        return current_visit, total_visits
    else:
        return False, False


def is_not_last_voucher_visit(current_visit: int, total_visits: int) -> bool:
    return current_visit < total_visits


def get_patients_who_did_not_use_their_voucher(df: pd.DataFrame) -> pd.DataFrame:
    """
    Find patients who have bought a voucher with multiple visits but did not make use
    of all the visits. The note about which visit is the current one is in the bookingNote column
    as e.g. 3/4 (third visit from four overall).
    Add column signaling the expiration (validity) of the voucher
    (4 months for 3 visits, 8 months for 5 visits) = isValidVoucher
    and another for expiration date = isValidUntil.
    """
    df = df.copy()
    df = df.dropna(subset=["bookingNote"])  # filter rows with bookingNote
    df["bookingNote"] = df["bookingNote"].astype(str)

    # filter rows with info about the number of visit in bookingNote
    df[['currentVisit', 'totalVisits']] = df.apply(
        lambda row: pd.Series(get_visits(row['bookingNote'])),
        axis=1
    )
    df = df[df['currentVisit'] != False]

    # rows with last visits
    last_visit_with_voucher = df.loc[df.groupby("customer")["start"].idxmax()]

    # rows with not completed vouchers
    not_used_vouchers = last_visit_with_voucher[is_not_last_voucher_visit(
        last_visit_with_voucher["currentVisit"], last_visit_with_voucher["totalVisits"])].copy()

    # we fill the column voucherStart with placeholders
    not_used_vouchers["voucherStart"] = pd.Series([pd.Timestamp.utcnow() for _ in range(len(not_used_vouchers))])
    # for each customer with not used voucher, find the last index of row with the lowest number of visit
    # in all visits with info about the number of visit in bookingNote
    for customer in not_used_vouchers["customer"]:
        counter = 1
        last_index = None
        while last_index is None:
            last_index = (df.loc[(df["customer"] == customer) &
                                 (df["bookingNote"].str.contains(rf"{counter}[\/\\]\d+"))]
                          .last_valid_index())
            counter += 1
        # get the start =~ when the voucher started to be valid
        voucher_start = df.at[last_index, "start"]
        # store it with the matching customer in not_used_vouchers
        not_used_vouchers.loc[not_used_vouchers["customer"] == customer, "voucherStart"] = voucher_start

    not_used_vouchers["isValidVoucher"] = not_used_vouchers.apply(
        lambda row: pd.Series(
            get_validity(row["voucherStart"], row["totalVisits"])[0]),
        axis=1)

    not_used_vouchers["isValidUntil"] = not_used_vouchers.apply(
        lambda row: pd.Series(
            get_validity(row["voucherStart"], row["totalVisits"])[1]),
        axis=1)

    return not_used_vouchers.sort_index()


if __name__ == '__main__':
    df = pd.read_csv(settings.RESERVANTO_DIR / "reservanto.csv", sep=";", encoding="utf-8")
    df = df[["title", "createdAt", "start", "end",
             "bookingNote", "customerId", "customer", "customerContact",
             "bookingNoShowState", "hasCustomerNote", "isFreeTime", "noShowStatus"]]
    pd.set_option("display.max_columns", None)
    aaa = get_patients_who_did_not_use_their_voucher(df)
    print(aaa.head())
    print(aaa["voucherStart"])
    print(aaa.info())
