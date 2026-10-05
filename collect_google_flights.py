import os
import json
import time
import sqlite3
from pathlib import Path
from datetime import datetime, date
from zoneinfo import ZoneInfo

import requests
import pandas as pd
from dotenv import load_dotenv


# ============================================================
# AIRPULSE - GOOGLE FLIGHTS COLLECTOR
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "google_flights_raw"
PROCESSED_DIR = DATA_DIR / "processed"
DB_PATH = DATA_DIR / "airpulse_google_flights.db"

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# Campaign
START_DATE = date(2026, 10, 4)
END_DATE = date(2026, 10, 13)

# Ngày bay cố định
TARGET_FLIGHT_DATE = "2026-10-14"

CURRENCY = "VND"

# ============================================================
# TEST MODE
# ============================================================

# True  = chỉ crawl SGN -> HAN = 1 search
# False = crawl đủ 8 routes
TEST_MODE = False


CORE_ROUTES = [
    ("SGN", "HAN"),
    ("HAN", "SGN"),

    ("SGN", "DAD"),
    ("HAN", "DAD"),

    ("SGN", "DLI"),
    ("HAN", "DLI"),

    ("SGN", "PQC"),
    ("HAN", "PQC"),
]


if TEST_MODE:
    ROUTES = [
        ("SGN", "HAN")
    ]
else:
    ROUTES = CORE_ROUTES


# ============================================================
# LOAD API KEYS
# ============================================================

load_dotenv(BASE_DIR / ".env")

ACCOUNTS = {
    "A": os.getenv("SERPAPI_KEY_A"),
    "B": os.getenv("SERPAPI_KEY_B"),
    "C": os.getenv("SERPAPI_KEY_C"),
}

for name, key in ACCOUNTS.items():
    if not key:
        raise RuntimeError(
            f"Không tìm thấy SERPAPI_KEY_{name} trong .env"
        )


# ============================================================
# CREATE FOLDERS
# ============================================================

RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# CHECK ACCOUNT QUOTA
# ============================================================

def get_account_remaining(account_name, api_key):

    response = requests.get(
        "https://serpapi.com/account.json",
        params={
            "api_key": api_key
        },
        timeout=30
    )

    if response.status_code != 200:
        return None

    data = response.json()

    remaining = data.get(
        "total_searches_left"
    )

    return remaining


def select_account(required_searches):

    available = {}

    print()
    print("CHECKING SERPAPI ACCOUNTS")
    print("-" * 50)

    for account_name, api_key in ACCOUNTS.items():

        try:

            remaining = get_account_remaining(
                account_name,
                api_key
            )

            if remaining is None:
                print(
                    f"{account_name}: không đọc được quota"
                )
                continue

            print(
                f"{account_name}: {remaining} searches left"
            )

            if remaining >= required_searches:
                available[account_name] = {
                    "remaining": remaining,
                    "api_key": api_key
                }

        except Exception as e:

            print(
                f"{account_name}: ERROR {e}"
            )

    if not available:
        raise RuntimeError(
            "Không có account nào đủ quota cho run này."
        )

    selected = max(
        available,
        key=lambda name:
            available[name]["remaining"]
    )

    return (
        selected,
        available[selected]["api_key"],
        available[selected]["remaining"]
    )


# ============================================================
# DATABASE
# ============================================================

def connect_db():

    conn = sqlite3.connect(DB_PATH)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS flights (

            flight_id TEXT PRIMARY KEY,

            airline TEXT,
            flight_number TEXT,

            origin TEXT,
            destination TEXT,

            departure_at TEXT,
            arrival_at TEXT,

            flight_date TEXT,

            first_seen_at TEXT,
            last_seen_at TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS fare_observations (

            observation_id INTEGER PRIMARY KEY AUTOINCREMENT,

            run_id TEXT NOT NULL,

            flight_id TEXT NOT NULL,

            observed_at TEXT NOT NULL,
            observation_date TEXT NOT NULL,
            observation_hour INTEGER NOT NULL,

            flight_date TEXT NOT NULL,
            days_to_departure INTEGER,

            origin TEXT NOT NULL,
            destination TEXT NOT NULL,

            airline TEXT,
            flight_number TEXT,

            departure_at TEXT,
            arrival_at TEXT,

            price INTEGER,
            currency TEXT,

            source TEXT,
            price_type TEXT,

            account_used TEXT,

            FOREIGN KEY (flight_id)
                REFERENCES flights(flight_id),

            UNIQUE(run_id, flight_id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS request_log (

            request_id INTEGER PRIMARY KEY AUTOINCREMENT,

            run_id TEXT,
            requested_at TEXT,

            account_used TEXT,

            origin TEXT,
            destination TEXT,
            flight_date TEXT,

            http_status INTEGER,

            direct_flights_found INTEGER,

            raw_file TEXT,

            error_message TEXT
        )
    """)

    conn.commit()

    return conn


# ============================================================
# FLIGHT ID
# ============================================================

def make_flight_id(
    flight_number,
    origin,
    destination,
    departure_at
):

    clean_number = (
        str(flight_number)
        .strip()
        .replace(" ", "")
    )

    clean_departure = (
        str(departure_at)
        .replace("-", "")
        .replace(":", "")
        .replace(" ", "T")
    )

    return (
        f"{clean_number}_"
        f"{origin}_"
        f"{destination}_"
        f"{clean_departure}"
    )


# ============================================================
# GOOGLE FLIGHTS SEARCH
# ============================================================

def search_google_flights(
    api_key,
    origin,
    destination
):

    url = "https://serpapi.com/search.json"

    params = {
        "engine": "google_flights",

        "departure_id": origin,
        "arrival_id": destination,

        "outbound_date": TARGET_FLIGHT_DATE,

        # one-way
        "type": 2,

        # economy
        "travel_class": 1,

        # one adult
        "adults": 1,

        # direct only
        "stops": 1,

        "currency": CURRENCY,

        "hl": "vi",
        "gl": "vn",

        "show_hidden": "true",

        "api_key": api_key,
    }

    return requests.get(
        url,
        params=params,
        timeout=90
    )


# ============================================================
# PARSE RESPONSE
# ============================================================

def extract_direct_flights(
    data,
    origin,
    destination
):

    offers = []

    offers.extend(
        data.get("best_flights", [])
    )

    offers.extend(
        data.get("other_flights", [])
    )

    # flight_id -> record
    unique_flights = {}

    for offer in offers:

        segments = offer.get(
            "flights",
            []
        )

        # Chỉ giữ flight direct thực sự
        if len(segments) != 1:
            continue

        flight = segments[0]

        departure = flight.get(
            "departure_airport",
            {}
        )

        arrival = flight.get(
            "arrival_airport",
            {}
        )

        actual_origin = departure.get("id")
        actual_destination = arrival.get("id")

        if actual_origin != origin:
            continue

        if actual_destination != destination:
            continue

        flight_number = flight.get(
            "flight_number"
        )

        airline = flight.get(
            "airline"
        )

        departure_at = departure.get(
            "time"
        )

        arrival_at = arrival.get(
            "time"
        )

        price = offer.get(
            "price"
        )

        if not flight_number:
            continue

        if not departure_at:
            continue

        flight_id = make_flight_id(
            flight_number,
            origin,
            destination,
            departure_at
        )

        record = {
            "flight_id": flight_id,

            "airline": airline,
            "flight_number": flight_number,

            "origin": origin,
            "destination": destination,

            "departure_at": departure_at,
            "arrival_at": arrival_at,

            "price": price,
        }

        # Nếu Google trả cùng flight nhiều lần,
        # giữ giá thấp nhất có giá trị.
        if flight_id not in unique_flights:

            unique_flights[
                flight_id
            ] = record

        else:

            old_price = unique_flights[
                flight_id
            ]["price"]

            new_price = price

            if (
                new_price is not None
                and (
                    old_price is None
                    or new_price < old_price
                )
            ):

                unique_flights[
                    flight_id
                ] = record

    return list(
        unique_flights.values()
    )


# ============================================================
# SAVE DATABASE
# ============================================================

def save_results(
    conn,
    flights,
    run_id,
    observed_at,
    account_used
):

    target_date = datetime.strptime(
        TARGET_FLIGHT_DATE,
        "%Y-%m-%d"
    ).date()

    days_to_departure = (
        target_date
        - observed_at.date()
    ).days

    new_flights = 0
    new_observations = 0

    for flight in flights:

        existed = conn.execute(
            """
            SELECT 1
            FROM flights
            WHERE flight_id = ?
            """,
            (
                flight["flight_id"],
            )
        ).fetchone()

        if not existed:

            conn.execute(
                """
                INSERT INTO flights (

                    flight_id,

                    airline,
                    flight_number,

                    origin,
                    destination,

                    departure_at,
                    arrival_at,

                    flight_date,

                    first_seen_at,
                    last_seen_at
                )

                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    flight["flight_id"],

                    flight["airline"],
                    flight["flight_number"],

                    flight["origin"],
                    flight["destination"],

                    flight["departure_at"],
                    flight["arrival_at"],

                    TARGET_FLIGHT_DATE,

                    observed_at.isoformat(),
                    observed_at.isoformat(),
                )
            )

            new_flights += 1

        else:

            conn.execute(
                """
                UPDATE flights

                SET
                    airline = ?,
                    arrival_at = ?,
                    last_seen_at = ?

                WHERE flight_id = ?
                """,
                (
                    flight["airline"],
                    flight["arrival_at"],
                    observed_at.isoformat(),
                    flight["flight_id"],
                )
            )

        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO fare_observations (

                run_id,

                flight_id,

                observed_at,
                observation_date,
                observation_hour,

                flight_date,
                days_to_departure,

                origin,
                destination,

                airline,
                flight_number,

                departure_at,
                arrival_at,

                price,
                currency,

                source,
                price_type,

                account_used
            )

            VALUES (
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?
            )
            """,
            (
                run_id,

                flight["flight_id"],

                observed_at.isoformat(),
                observed_at.date().isoformat(),
                observed_at.hour,

                TARGET_FLIGHT_DATE,
                days_to_departure,

                flight["origin"],
                flight["destination"],

                flight["airline"],
                flight["flight_number"],

                flight["departure_at"],
                flight["arrival_at"],

                flight["price"],
                CURRENCY,

                "SerpAPI Google Flights",
                "current_observed",

                account_used,
            )
        )

        if cursor.rowcount > 0:
            new_observations += 1

    conn.commit()

    return new_flights, new_observations


# ============================================================
# EXPORT CSV
# ============================================================

def export_csv(conn):

    flights_df = pd.read_sql_query(
        """
        SELECT *
        FROM flights

        ORDER BY
            flight_date,
            origin,
            destination,
            departure_at
        """,
        conn
    )

    observations_df = pd.read_sql_query(
        """
        SELECT *
        FROM fare_observations

        ORDER BY
            observed_at,
            origin,
            destination,
            departure_at
        """,
        conn
    )

    flights_df.to_csv(
        PROCESSED_DIR
        / "google_flights.csv",

        index=False,
        encoding="utf-8-sig"
    )

    observations_df.to_csv(
        PROCESSED_DIR
        / "google_fare_observations.csv",

        index=False,
        encoding="utf-8-sig"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    now = datetime.now(VN_TZ)

    print()
    print("=" * 70)
    print("AIRPULSE - GOOGLE FLIGHTS COLLECTOR")
    print("=" * 70)

    print(
        "Mode       :",
        "TEST" if TEST_MODE else "FULL"
    )

    print(
        "Observed   :",
        now.strftime("%Y-%m-%d %H:%M:%S")
    )

    print(
        "Flight date:",
        TARGET_FLIGHT_DATE
    )

    print(
        "Routes     :",
        len(ROUTES)
    )

    print()

    if not (
        START_DATE
        <= now.date()
        <= END_DATE
    ):

        print(
            "STOP: Ngoài thời gian collection."
        )

        return

    account_name, api_key, remaining = (
        select_account(
            required_searches=len(ROUTES)
        )
    )

    print()
    print(
        "SELECTED ACCOUNT:",
        account_name
    )

    print(
        "ACCOUNT REMAINING:",
        remaining
    )

    run_id = (
        f"{account_name}_"
        f"{now.strftime('%Y%m%d_%H%M%S')}"
    )

    raw_run_dir = (
        RAW_DIR
        / now.strftime("%Y%m%d")
        / now.strftime("%H%M%S")
    )

    raw_run_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    conn = connect_db()

    total_found = 0
    total_new_flights = 0
    total_new_observations = 0

    for index, (
        origin,
        destination
    ) in enumerate(
        ROUTES,
        start=1
    ):

        print()
        print(
            f"[{index}/{len(ROUTES)}] "
            f"{origin} -> {destination}"
        )

        requested_at = datetime.now(
            VN_TZ
        )

        try:

            response = search_google_flights(
                api_key,
                origin,
                destination
            )

            print(
                "HTTP:",
                response.status_code
            )

            if response.status_code != 200:

                error_text = (
                    response.text[:500]
                )

                conn.execute(
                    """
                    INSERT INTO request_log (

                        run_id,
                        requested_at,

                        account_used,

                        origin,
                        destination,
                        flight_date,

                        http_status,

                        direct_flights_found,

                        raw_file,

                        error_message
                    )

                    VALUES (
                        ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?
                    )
                    """,
                    (
                        run_id,
                        requested_at.isoformat(),

                        account_name,

                        origin,
                        destination,
                        TARGET_FLIGHT_DATE,

                        response.status_code,

                        0,

                        None,

                        error_text,
                    )
                )

                conn.commit()

                continue

            data = response.json()

            raw_file = (
                raw_run_dir
                / (
                    f"{origin}_"
                    f"{destination}_"
                    f"{TARGET_FLIGHT_DATE}.json"
                )
            )

            with open(
                raw_file,
                "w",
                encoding="utf-8"
            ) as file:

                json.dump(
                    data,
                    file,
                    ensure_ascii=False,
                    indent=2
                )

            flights = extract_direct_flights(
                data,
                origin,
                destination
            )

            print(
                "Direct flights:",
                len(flights)
            )

            new_flights, new_observations = (
                save_results(
                    conn,
                    flights,
                    run_id,
                    requested_at,
                    account_name
                )
            )

            print(
                "New flights:",
                new_flights
            )

            print(
                "Fare observations:",
                new_observations
            )

            total_found += len(
                flights
            )

            total_new_flights += (
                new_flights
            )

            total_new_observations += (
                new_observations
            )

            conn.execute(
                """
                INSERT INTO request_log (

                    run_id,
                    requested_at,

                    account_used,

                    origin,
                    destination,
                    flight_date,

                    http_status,

                    direct_flights_found,

                    raw_file,

                    error_message
                )

                VALUES (
                    ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?
                )
                """,
                (
                    run_id,
                    requested_at.isoformat(),

                    account_name,

                    origin,
                    destination,
                    TARGET_FLIGHT_DATE,

                    response.status_code,

                    len(flights),

                    str(raw_file),

                    None,
                )
            )

            conn.commit()

        except Exception as e:

            print(
                "ERROR:",
                str(e)
            )

        time.sleep(1)

    export_csv(conn)

    conn.close()

    print()
    print("=" * 70)
    print("COLLECTION COMPLETE")
    print("=" * 70)

    print(
        "Account:",
        account_name
    )

    print(
        "Flights observed:",
        total_found
    )

    print(
        "New flights:",
        total_new_flights
    )

    print(
        "New observations:",
        total_new_observations
    )

    print()
    print(
        "Database:",
        DB_PATH
    )

    print(
        "Observations CSV:",
        PROCESSED_DIR
        / "google_fare_observations.csv"
    )

    print(
        "Raw JSON:",
        raw_run_dir
    )

    print("=" * 70)


if __name__ == "__main__":
    main()
