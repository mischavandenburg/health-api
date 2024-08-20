import psycopg2
from psycopg2.extras import execute_values
import requests
import os
import json


def connect_to_db():
    return psycopg2.connect(
        dbname="postgres", user="postgres", password="postgres", host="localhost"
    )


def fetch_oura_data(token, start_date, end_date):
    url = "https://api.ouraring.com/v2/usercollection/sleep"
    params = {"start_date": start_date, "end_date": end_date}
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers, params=params)
    return response.json()["data"]


def prepare_sleep_data(data):
    prepared_data = []
    for item in data:
        sleep_data = {
            "id": item["id"],
            "average_breath": item["average_breath"],
            "average_heart_rate": item["average_heart_rate"],
            "average_hrv": item["average_hrv"],
            "awake_time": item["awake_time"],
            "bedtime_end": item["bedtime_end"],
            "bedtime_start": item["bedtime_start"],
            "day": item["day"],
            "deep_sleep_duration": item["deep_sleep_duration"],
            "efficiency": item["efficiency"],
            "latency": item["latency"],
            "light_sleep_duration": item["light_sleep_duration"],
            "lowest_heart_rate": item["lowest_heart_rate"],
            "rem_sleep_duration": item["rem_sleep_duration"],
            "restless_periods": item["restless_periods"],
            "sleep_score_delta": item["sleep_score_delta"],
            "time_in_bed": item["time_in_bed"],
            "total_sleep_duration": item["total_sleep_duration"],
            "type": item["type"],
            # "hrv": json.dumps(item["hrv"]),
            # "movement_30_sec": item["movement_30_sec"],
            # "readiness": json.dumps(item["readiness"]),
            # "sleep_phase_5_min": item["sleep_phase_5_min"],
        }
        prepared_data.append(sleep_data)
    return prepared_data


def upsert_sleep_data(cursor, data):
    columns = data[0].keys()
    sql = f"""
    INSERT INTO sleep_data ({', '.join(columns)})
    VALUES %s
    ON CONFLICT (id) DO UPDATE SET
    {', '.join(f"{col} = EXCLUDED.{col}" for col in columns if col != 'id')}
    """
    values = [[item[col] for col in columns] for item in data]
    execute_values(cursor, sql, values)


def main():
    token = os.getenv("OURA_TOKEN")
    start_date = "2024-08-18"
    end_date = "2024-08-20"

    conn = connect_to_db()
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sleep_data (
        id TEXT PRIMARY KEY,
        average_breath FLOAT,
        average_heart_rate FLOAT,
        average_hrv INTEGER,
        awake_time INTEGER,
        bedtime_end TIMESTAMP,
        bedtime_start TIMESTAMP,
        day DATE,
        deep_sleep_duration INTEGER,
        efficiency INTEGER,
        latency INTEGER,
        light_sleep_duration INTEGER,
        low_battery_alert BOOLEAN,
        lowest_heart_rate INTEGER,
        period INTEGER,
        readiness_score INTEGER,
        readiness_score_delta INTEGER,
        rem_sleep_duration INTEGER,
        restless_periods INTEGER,
        sleep_score_delta INTEGER,
        sleep_algorithm_version TEXT,
        time_in_bed INTEGER,
        total_sleep_duration INTEGER,
        type TEXT,
        heart_rate JSONB,
        hrv JSONB,
        movement_30_sec TEXT,
        readiness JSONB,
        sleep_phase_5_min TEXT
    )
    """)

    raw_data = fetch_oura_data(token, start_date, end_date)
    prepared_data = prepare_sleep_data(raw_data)
    upsert_sleep_data(cursor, prepared_data)

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Data for {len(prepared_data)} sleep sessions inserted/updated.")


if __name__ == "__main__":
    main()
