import pandas as pd
import requests
import io
import psycopg2
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import os
import sys
import re

sys.stdout.reconfigure(encoding='utf-8')
#load environments variables
load_dotenv()

#kobo_environments
KOBO_USERNAME=os.getenv("KOBO_USERNAME")
KOBO_PASSWORD=os.getenv("KOBO_PASSWORD")
KOBO_CSV_URL="https://kf.kobotoolbox.org/api/v2/assets/a6bxXmYVoEaoGgzqQh5qUE/export-settings/esdv3TzzMWEwWJmQ43G5kcC/data.csv"

#Postgre Credenetials 
PG_HOST=os.getenv("PG_HOST")
PG_DATABASE=os.getenv("PG_DATABASE")
PG_USER=os.getenv("PG_USER")
PG_PASSWORD=os.getenv("PG_PASSWORD")
PG_PORT=os.getenv("PG_PORT")
#Schema and table deatils
Schema_name="war"
table_name="russia_ukraine_conflict"     #special characters avoided

#Step1: Fetch details form kobo Toolbox
print("fetching data from kobo Toolbox...")
response=requests.get(KOBO_CSV_URL,auth=HTTPBasicAuth(KOBO_USERNAME,KOBO_PASSWORD))

if response.status_code == 200:
    print("✅data received successfully")

    csv_data = io.StringIO(response.text)
    df = pd.read_csv(csv_data, sep=';', on_bad_lines='skip')

    print("processing data")
    df.columns = [re.sub(r'\W+', '_', col.strip()) for col in df.columns]
    df.columns = [col.strip().replace("&", "and").replace("-", "_") for col in df.columns]

    df["Total_Casualties"] = df[["Casualties", "Injured", "Captured"]].sum(axis=1)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    print("Uploading data to PostgreSQL...")

    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT
    )
    cur = conn.cursor()

    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {Schema_name};")
    cur.execute(f"DROP TABLE IF EXISTS {Schema_name}.{table_name}")

    cur.execute(f"""
    CREATE TABLE {Schema_name}.{table_name}(
        id SERIAL PRIMARY KEY,
        "start" TIMESTAMP,
        "end" TIMESTAMP,
        "date" DATE,
        country TEXT,
        event TEXT,
        oblast TEXT,
        casualties INT,
        injured INT,
        captured INT,
        civilian_casualties INT,
        new_recruits INT,
        combat_intensity FLOAT,
        territory_status TEXT,
        percentage_occupied FLOAT,
        area_occupied FLOAT,
        total_casualties INT
    )
    """)

    insert_query = f"""
    INSERT INTO {Schema_name}.{table_name}(
        "start", "end", "date", country, event, oblast,
        casualties, injured, captured,
        civilian_casualties, new_recruits, combat_intensity,
        territory_status, percentage_occupied, area_occupied,
        total_casualties
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for _, row in df.iterrows():
        cur.execute(insert_query, (
            row.get("start"),
            row.get("end"),
            row.get("Date"),
            row.get("Country"),
            row.get("Event"),
            row.get("Oblast"),
            row.get("Casualties", 0),
            row.get("Injured", 0),
            row.get("Captured", 0),
            row.get("Civilian_Casualties", 0),
            row.get("New_Recruits", 0),
            row.get("Combat_Intensity", 0.0),
            row.get("Territory_Status"),
            row.get("Percentage_Occupied", 0.0),
            row.get("Area_Occupied", 0.0),
            row.get("Total_Casualties", 0)
        ))

    conn.commit()
    cur.close()
    conn.close()

    print(" ✅Data successfully loaded into PostgreSQL!")

else:
    print(f" ❌Failed to fetch data. Status code: {response.status_code}")
