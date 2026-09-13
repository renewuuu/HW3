from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import requests
import pandas as pd


def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    # Get Snowflake connection
    conn = hook.get_conn()
    return conn.cursor()


@task
def extract(url):

    latitude = Variable.get("latitude")
    longitude = Variable.get("longitude")

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "past_days": 60,
        "forecast_days": 0,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "weather_code"
        ],
        "timezone": "America/Los_Angeles"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    return response.json()


@task
def transform(data):

    df = pd.DataFrame({
        "date": data["daily"]["time"],
        "temp_max": data["daily"]["temperature_2m_max"],
        "temp_min": data["daily"]["temperature_2m_min"],
        "precipitation": data["daily"]["precipitation_sum"],
        "weather_code": data["daily"]["weather_code"]
    })

    df["date"] = pd.to_datetime(df["date"]).dt.date

    return df.to_json()


@task
def load(data, target_table):

    df = pd.read_json(data)

    latitude = Variable.get("latitude")
    longitude = Variable.get("longitude")

    cur = return_snowflake_conn()

    try:
        cur.execute("BEGIN;")

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                latitude FLOAT,
                longitude FLOAT,
                date DATE,
                temp_max FLOAT,
                temp_min FLOAT,
                precipitation FLOAT,
                weather_code INTEGER,
                PRIMARY KEY (latitude, longitude, date)
            )
        """)

        cur.execute(f"DELETE FROM {target_table}")

        for r in df.itertuples(index=False, name=None):

            date = r[0]
            temp_max = r[1]
            temp_min = r[2]
            precipitation = r[3]
            weather_code = r[4]

            sql = f"""
                INSERT INTO {target_table}
                (
                    latitude,
                    longitude,
                    date,
                    temp_max,
                    temp_min,
                    precipitation,
                    weather_code
                )
                VALUES (
                    {latitude},
                    {longitude},
                    '{date}',
                    {temp_max},
                    {temp_min},
                    {precipitation},
                    {weather_code}
                )
            """

            cur.execute(sql)

        cur.execute("COMMIT;")

        print(f"Successfully loaded {len(df)} records.")

    except Exception as e:

        cur.execute("ROLLBACK;")

        print(e)

        raise e


with DAG(
    dag_id='HW3',
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=['ETL'],
    schedule=None
) as dag:

    url = "https://api.open-meteo.com/v1/forecast"

    target_table = "RENE_DB.RAW.WEATHER_DATA"

    data = extract(url)
    lines = transform(data)
    load(lines, target_table)