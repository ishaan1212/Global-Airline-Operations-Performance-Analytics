import requests
import snowflake.connector
import json
import time
from datetime import datetime

# Snowflake Credentials
SNOWFLAKE_ACCOUNT = "your_url"
SNOWFLAKE_USER = "you_username"
SNOWFLAKE_PASSWORD = "your_password"
SNOWFLAKE_DATABASE = "FLIGHT_DB"
SNOWFLAKE_SCHEMA = "PUBLIC"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_TABLE = "Flights"

# OpenSky API Endpoint for live flight data
url = "https://opensky-network.org/api/states/all"

def fetch_flight_data():
    """Fetch real-time flight data from OpenSky API."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get("states", [])
    except requests.exceptions.RequestException as e:
        print(f"🚨 API Request Error: {e}")
        return []

def insert_into_snowflake(flights):
    """Insert fetched flight data into Snowflake."""
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            warehouse=SNOWFLAKE_WAREHOUSE
        )

        cursor = conn.cursor()

        sql = f"""
            INSERT INTO {SNOWFLAKE_TABLE} (
                FlightID, Callsign, OriginCountry, LastContactTime, LastPositionUpdate,
                Longitude, Latitude, BarometricAltitude, OnGround, Velocity, Heading
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for flight in flights[:10]: 
            values = (
                flight[0],  # ICAO24 Aircraft ID
                flight[1] if flight[1] else "UNKNOWN",
                flight[2] if flight[2] else "UNKNOWN",
                datetime.utcfromtimestamp(flight[3]).strftime('%Y-%m-%d %H:%M:%S'),
                datetime.utcfromtimestamp(flight[4]).strftime('%Y-%m-%d %H:%M:%S'),
                flight[5] if flight[5] else None,
                flight[6] if flight[6] else None,
                flight[7] if flight[7] else None,
                flight[8],
                flight[9] if flight[9] else None,
                flight[10] if flight[10] else None
            )

            cursor.execute(sql, values)
            time.sleep(1)

        conn.commit()
        cursor.close()
        conn.close()

        print("✅ Flight data inserted into Snowflake successfully!")

    except snowflake.connector.errors.OperationalError as e:
        print(f"🚨 Snowflake Connection Error: {e}")
    except Exception as e:
        print(f"🚨 Unexpected Error: {e}")

if __name__ == "__main__":
    flights = fetch_flight_data()
    if flights:
        insert_into_snowflake(flights)
