import requests
import pandas as pd

BASE_URL = "https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CD"

PARAMS = {
    "format": "json",
    "per_page": 1000
}

MAX_RECORDS = 100000


def fetch_world_bank_data():
    all_data = []
    page = 1

    while len(all_data) < MAX_RECORDS:
        PARAMS["page"] = page
        response = requests.get(BASE_URL, params=PARAMS, timeout=30)

        if response.status_code != 200:
            break

        data = response.json()

        if len(data) < 2:
            break

        metadata = data[0]
        records = data[1]

        if not records:
            break

        all_data.extend(records)

        if page >= metadata["pages"]:
            break

        page += 1

    return all_data[:MAX_RECORDS]


def transform_data(raw_data):
    df = pd.json_normalize(raw_data)

    df = df.rename(columns={
        "country.value": "country_name",
        "countryiso3code": "country_code",
        "indicator.id": "indicator_code",
        "indicator.value": "indicator_name",
        "date": "year",
        "value": "indicator_value"
    })

    df = df.dropna(subset=["indicator_value"])
    return df