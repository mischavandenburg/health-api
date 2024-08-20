import json
import psycopg2
from psycopg2.extras import execute_values

import requests
import os

# from rich import print

token = os.getenv("OURA_TOKEN")


# Sleep

url = "https://api.ouraring.com/v2/usercollection/sleep"
params = {"start_date": "2024-08-18", "end_date": "2024-08-20"}
headers = {"Authorization": f"Bearer {token}"}
response = requests.request("GET", url, headers=headers, params=params)
print(response.text)


# url = "https://api.ouraring.com/v2/usercollection/heartrate"
# params = {"start_date": "2024-08-18", "end_date": "2024-08-20"}
# headers = {"Authorization": f"Bearer {token}"}
# response = requests.request("GET", url, headers=headers, params=params)
# print(response.text)

data = response.json()


def connect_to_db():
    return psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="postgres",
        host="localhost"
    )


def upsert_sleep_data(cursor, data):
    columns = data[0].keys()
    sql = f"""
    INSERT INTO sleep_data ({', '.join(columns)})
    VALUES %s
    ON CONFLICT (id) DO UPDATE SET
    {', '.join(f"{col} = EXCLUDED.{col}" for col in columns if col != 'id')}
    """
    values = [[d[col] for col in columns] for d in data]
    execute_values(cursor, sql, values)


def main():

    conn = connect_to_db()
    cursor = conn.cursor()

    upsert_sleep_data(cursor, data)

    # Here you would also process heart_rate_data, hrv_data, and readiness_data

    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
