import hashlib
import io
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


# ================================================================
# CONFIGURATION
# ================================================================

SWISSGRID_PAGE = (
    "https://www.swissgrid.ch/en/home/customers/topics/"
    "energy-data-ch.html"
)

POLL_SECONDS = 60

# The current CSV is uploaded completely after every restart.
FORCE_INITIAL_SYNC = True

STATE_FILE = Path("swissgrid_state.json")

# You can either set these as environment variables or replace
# the default values directly below.
INFLUX_URL = "http://10.30.4.110:8086"
INFLUX_TOKEN = "F0cssT5LQoTFZXMZGiBxPU9CIZXFOM6S1xUnAnliDO3KHTHZ8_NgoLyPZvX6sEO60YznAEoAoSnZ6t5RwcLUjg=="
INFLUX_ORG = "hevs"
INFLUX_BUCKET = "grid_data"

INFLUX_MEASUREMENT = ("swissgrid_control_area_balance")



# ================================================================
# SWISSGRID COLUMN MAPPING
# ================================================================

COLUMN_MAP = {
    "Date Time [UTC]": "timestamp_utc",

    "Abgedeckte Bedarf der aFRR+":
        "afrr_activation_positive_MW",

    "Abgedeckte Bedarf der aFRR-":
        "afrr_activation_negative_MW",

    "Abgedeckte Bedarf der SA mFRR+":
        "mfrr_scheduled_positive_MW",

    "Abgedeckte Bedarf der SA mFRR-":
        "mfrr_scheduled_negative_MW",

    "Abgedeckte Bedarf der DA mFRR+":
        "mfrr_activation_positive_MW",

    "Abgedeckte Bedarf der DA mFRR-":
        "mfrr_activation_negative_MW",

    "NRV+ (Import)":
        "NRV_import_MW",

    "NRV- (Export)":
        "NRV_export_MW",

    "FRCE+ (Import)":
        "FRCE_import_MW",

    "FRCE- (Export)":
        "FRCE_export_MW",

    "Total System Imbalance":
        "total_system_imbalance_MW",

    "AE-Preis":
        "imbalance_price_eur_ct_kWh",
}


# ================================================================
# STATE
# ================================================================

def empty_source_state():
    return {
        "csv_url": None,
        "row_signatures": {},
    }


def load_state():
    if not STATE_FILE.exists():
        return {
            "influx_bucket": None,
            "yearly": empty_source_state(),
            "daily": empty_source_state(),
        }

    try:
        return json.loads(
            STATE_FILE.read_text(
                encoding="utf-8"
            )
        )
    except Exception:
        return {
            "influx_bucket": None,
            "yearly": empty_source_state(),
            "daily": empty_source_state(),
        }


def save_state(state):
    temporary_file = STATE_FILE.with_suffix(".tmp")

    temporary_file.write_text(
        json.dumps(
            state,
            indent=2,
        ),
        encoding="utf-8",
    )

    temporary_file.replace(STATE_FILE)


# ================================================================
# FIND SWISSGRID YEARLY AND DAILY FILES
# ================================================================

def get_csv_urls(session, year):
    response = session.get(
        SWISSGRID_PAGE,
        timeout=30,
    )
    response.raise_for_status()

    soup = BeautifulSoup(
        response.text,
        "html.parser",
    )

    yearly_url = None
    daily_url = None

    yearly_text = (
        f"control area balance {year} (yearly)"
    )

    for link in soup.select("a[href]"):
        text = link.get_text(
            " ",
            strip=True,
        ).lower()

        href = urljoin(
            SWISSGRID_PAGE,
            link["href"],
        )

        if (
            "control area balance (daily)" in text
            and ".csv" in href.lower()
        ):
            daily_url = href

        if (
            yearly_text in text
            and ".csv" in href.lower()
        ):
            yearly_url = href

    if yearly_url is None:
        raise RuntimeError(
            f"Yearly CSV for {year} was not found"
        )

    if daily_url is None:
        raise RuntimeError(
            "Daily Swissgrid CSV was not found"
        )

    return yearly_url, daily_url


# ================================================================
# READ AND PREPARE CSV
# ================================================================

def download_csv(session, url):
    response = session.get(
        url,
        timeout=30,
    )
    response.raise_for_status()

    return response.content


def read_csv_file(content):
    dataframe = pd.read_csv(
        io.BytesIO(content),
        sep="\t",
        encoding="utf-8-sig",
    )

    if dataframe.shape[1] == 1:
        dataframe = pd.read_csv(
            io.BytesIO(content),
            sep=None,
            engine="python",
            encoding="utf-8-sig",
        )

    dataframe.columns = (
        dataframe.columns
        .astype(str)
        .str.strip()
    )

    return dataframe


def parse_timestamp(series):
    values = (
        series
        .astype(str)
        .str.strip()
    )

    timestamps = pd.to_datetime(
        values,
        format="%d.%m.%Y %H:%M",
        utc=True,
        errors="coerce",
    )

    missing = timestamps.isna()

    if missing.any():
        timestamps.loc[missing] = pd.to_datetime(
            values.loc[missing],
            format="mixed",
            dayfirst=True,
            utc=True,
            errors="coerce",
        )

    return timestamps


def convert_numeric(series):
    values = (
        series
        .astype(str)
        .str.strip()
    )

    values = values.replace(
        {
            "": None,
            "nan": None,
            "NaN": None,
            "-": None,
        }
    )

    values = values.str.replace(
        ",",
        ".",
        regex=False,
    )

    return pd.to_numeric(
        values,
        errors="coerce",
    )


def prepare_data(dataframe):
    dataframe.columns = (
        dataframe.columns
        .astype(str)
        .str.strip()
    )

    missing_columns = [
        column
        for column in COLUMN_MAP
        if column not in dataframe.columns
    ]

    if missing_columns:
        raise RuntimeError(
            "Missing columns: "
            + ", ".join(missing_columns)
        )

    result = pd.DataFrame()

    result["timestamp_utc"] = parse_timestamp(
        dataframe["Date Time [UTC]"]
    )

    for source_column, output_column in COLUMN_MAP.items():

        if source_column == "Date Time [UTC]":
            continue

        result[output_column] = convert_numeric(
            dataframe[source_column]
        )

    result = (
        result
        .dropna(subset=["timestamp_utc"])
        .sort_values("timestamp_utc")
        .drop_duplicates(
            subset=["timestamp_utc"],
            keep="last",
        )
        .reset_index(drop=True)
    )

    if result.empty:
        raise RuntimeError(
            "No valid Swissgrid data found"
        )

    return result


# ================================================================
# DETECT NEW OR CORRECTED ROWS
# ================================================================

def row_signature(row):
    values = {}

    for column in row.index:

        if column == "timestamp_utc":
            continue

        value = row[column]

        if pd.isna(value):
            values[column] = None
        else:
            values[column] = float(value)

    encoded = json.dumps(
        values,
        sort_keys=True,
    ).encode("utf-8")

    return hashlib.sha256(
        encoded
    ).hexdigest()


def identify_rows_to_send(
    dataframe,
    previous_signatures,
):
    current_signatures = {}
    indices_to_send = []

    for index, row in dataframe.iterrows():

        timestamp_key = (
            row["timestamp_utc"].isoformat()
        )

        signature = row_signature(row)

        current_signatures[timestamp_key] = (
            signature
        )

        if (
            previous_signatures.get(timestamp_key)
            != signature
        ):
            indices_to_send.append(index)

    rows_to_send = dataframe.loc[
        indices_to_send
    ].copy()

    return rows_to_send, current_signatures


# ================================================================
# INFLUXDB
# ================================================================

def create_points(dataframe):
    points = []

    for _, row in dataframe.iterrows():

        point = (
            Point(INFLUX_MEASUREMENT)
            .tag("area", "CH")
            .tag("source", "Swissgrid")
            .time(
                row["timestamp_utc"].to_pydatetime(),
                WritePrecision.S,
            )
        )

        for column in dataframe.columns:

            if column == "timestamp_utc":
                continue

            value = row[column]

            if pd.notna(value):
                point.field(
                    column,
                    float(value),
                )

        points.append(point)

    return points


def send_dataframe(write_api, dataframe):
    if dataframe.empty:
        return

    points = create_points(dataframe)

    write_api.write(
        bucket=INFLUX_BUCKET,
        org=INFLUX_ORG,
        record=points,
    )


# ================================================================
# MAIN
# ================================================================

def main():
    if not INFLUX_TOKEN:
        raise RuntimeError(
            "Set the INFLUX_TOKEN environment variable"
        )

    session = requests.Session()

    session.headers.update(
        {
            "User-Agent":
                "SwissgridInfluxMirror/1.0"
        }
    )

    state = load_state()

    with InfluxDBClient(
        url=INFLUX_URL,
        token=INFLUX_TOKEN,
        org=INFLUX_ORG,
    ) as influx_client:

        write_api = influx_client.write_api(
            write_options=SYNCHRONOUS
        )

        # --------------------------------------------------------
        # 1. Upload current-yearly data first
        # --------------------------------------------------------

        current_year = datetime.now(
            timezone.utc
        ).year

        yearly_url, daily_url = get_csv_urls(
            session,
            current_year,
        )

        yearly_content = download_csv(
            session,
            yearly_url,
        )

        yearly_dataframe = prepare_data(
            read_csv_file(yearly_content)
        )

        if FORCE_INITIAL_SYNC:
            yearly_previous_signatures = {}
        elif (
            state.get("influx_bucket")
            != INFLUX_BUCKET
        ):
            yearly_previous_signatures = {}
        elif (
            state["yearly"].get("csv_url")
            != yearly_url
        ):
            yearly_previous_signatures = {}
        else:
            yearly_previous_signatures = (
                state["yearly"].get(
                    "row_signatures",
                    {},
                )
            )

        yearly_to_send, yearly_signatures = (
            identify_rows_to_send(
                yearly_dataframe,
                yearly_previous_signatures,
            )
        )

        send_dataframe(
            write_api,
            yearly_to_send,
        )

        state["yearly"] = {
            "csv_url": yearly_url,
            "row_signatures": yearly_signatures,
        }

        save_state(state)

        print(
            "The available data of the current year "
            "are sent to InfluxDB."
        )

        # --------------------------------------------------------
        # 2. Upload daily data after yearly data
        # --------------------------------------------------------

        daily_content = download_csv(
            session,
            daily_url,
        )

        daily_dataframe = prepare_data(
            read_csv_file(daily_content)
        )

        # The daily file is deliberately uploaded fully
        # during the initial synchronization.
        daily_to_send, daily_signatures = (
            identify_rows_to_send(
                daily_dataframe,
                {},
            )
        )

        send_dataframe(
            write_api,
            daily_to_send,
        )

        state["influx_bucket"] = INFLUX_BUCKET

        state["daily"] = {
            "csv_url": daily_url,
            "row_signatures": daily_signatures,
        }

        save_state(state)

        if not daily_to_send.empty:
            last_timestamp = (
                daily_to_send[
                    "timestamp_utc"
                ].max()
            )

            print(
                "The last timestamp sent to InfluxDB "
                f"is {last_timestamp.isoformat()}."
            )

        # --------------------------------------------------------
        # 3. Continue monitoring daily data
        # --------------------------------------------------------

        while True:

            try:
                _, latest_daily_url = (
                    get_csv_urls(
                        session,
                        current_year,
                    )
                )

                latest_daily_content = download_csv(
                    session,
                    latest_daily_url,
                )

                latest_daily_dataframe = (
                    prepare_data(
                        read_csv_file(
                            latest_daily_content
                        )
                    )
                )

                previous_signatures = (
                    state["daily"].get(
                        "row_signatures",
                        {},
                    )
                    if state["daily"].get(
                        "csv_url"
                    )
                    == latest_daily_url
                    else {}
                )

                daily_to_send, daily_signatures = (
                    identify_rows_to_send(
                        latest_daily_dataframe,
                        previous_signatures,
                    )
                )

                if not daily_to_send.empty:

                    send_dataframe(
                        write_api,
                        daily_to_send,
                    )

                    state["daily"] = {
                        "csv_url": latest_daily_url,
                        "row_signatures":
                            daily_signatures,
                    }

                    save_state(state)

                    last_timestamp = (
                        daily_to_send[
                            "timestamp_utc"
                        ].max()
                    )

                    print(
                        "The last timestamp sent to "
                        "InfluxDB is "
                        f"{last_timestamp.isoformat()}."
                    )

            except Exception as error:
                print(f"Error: {error}")

            time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()