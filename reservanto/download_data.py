import json
import time

import pandas as pd
import requests

from reservanto.process_data import convert_date_to_unix_timestamp
from reservanto import settings


def fetch_data_to_dataframe(url: str, s: requests.Session):
    """
    Fetches data from the given URL and returns it as a Pandas DataFrame.

    Parameters:
    url (str): The URL to send the GET request to.
    s (requests.Session):

    Returns:
    pd.DataFrame: DataFrame containing the fetched data.
    """
    try:
        response = s.post(url)
        byte_data = response.content
        output = json.loads(byte_data.decode("utf-8"))  # Parse JSON response
        df = pd.json_normalize(output)

        return df

    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    start_time = convert_date_to_unix_timestamp(2024, 9, 30)
    end_time = time.time()  # now
    token = "placeholder"  # TODO ?!?!?!
    payload = {"UserName": settings.RESERVANTO_USERNAME, "Password": settings.RESERVANTO_PASSWORD}

    login_url = "https://merchant.reservanto.cz/Account/Login?ReturnUrl=%2F"
    url = (f"https://merchant.reservanto.cz/Calendar/Feed?start={start_time}&end={end_time}"
           f"&rsIds=36459&_selectedWindowSegmentId=1034&_={token}")

    with requests.Session() as s:
        login_response = s.post(login_url, data=payload, allow_redirects=True)
        login_response.raise_for_status()  # Raises exception when not a 2xx response

        df = fetch_data_to_dataframe(url, s)
        df.to_csv("reservanto.csv", sep=";", encoding="utf-8")
