from airflow.decorators import dag, task
from datetime import timedelta, datetime

import json
import requests
import psycopg2


default_args = {
    'owner': 'nindo',
    'retries': 1,
    'retry_delay': timedelta(seconds=60)
}


class PostgresDB:
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password

    def __enter__(self):
        self.conn = psycopg2.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password
        )
        self.cur = self.conn.cursor()
        return self.cur

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.commit()
        self.cur.close()
        self.conn.close()


@dag(dag_id='weather_to_postresql_v1', default_args=default_args, start_date=datetime(2023, 4, 1),
     schedule='5-50/15 * * * *', catchup=False)
def weather_to_psql():

    @task()
    def get_api_data():
        url = "https://weatherapi-com.p.rapidapi.com/current.json"
        querystring = {'q': '37.57, 126.98'}
        headers = {
            "X-RapidAPI-Key": RAPID_WEATHERAPI_KEY,
            "X-RapidAPI-Host": "weatherapi-com.p.rapidapi.com"
        }
        response = requests.request("GET", url, headers=headers, params=querystring)

        data = json.loads(response.text)
        data['current']['condition'] = data['current']['condition']['text']
        return data['current']

    @task()
    def upload_to_psql(weather_data: dict):
        secret_data = {
            'host': HOST_IP,
            'database': 'default_db',
            'user': PSQL_USER,
            'password': PSQL_PASSWORD
        }

        with PostgresDB(**secret_data) as cur:
            cur.execute("""CREATE TABLE IF NOT EXISTS weather_data (
                                id SERIAL PRIMARY KEY,
                                last_updated_epoch BIGINT,
                                last_updated TIMESTAMP,
                                temp_c DECIMAL(5,2),
                                temp_f DECIMAL(5,2),
                                is_day INTEGER,
                                condition VARCHAR(30),
                                wind_mph DECIMAL(5,2),
                                wind_kph DECIMAL(5,2),
                                wind_degree INTEGER,
                                wind_dir VARCHAR(10),
                                pressure_mb DECIMAL(6,2),
                                pressure_in DECIMAL(6,2),
                                precip_mm DECIMAL(6,2),
                                precip_in DECIMAL(6,2),
                                humidity INTEGER,
                                cloud INTEGER,
                                feelslike_c DECIMAL(5,2),
                                feelslike_f DECIMAL(5,2),
                                vis_km DECIMAL(6,2),
                                vis_miles DECIMAL(6,2),
                                uv DECIMAL(5,2),
                                gust_mph DECIMAL(5,2),
                                gust_kph DECIMAL(5,2)
                                );
                        """)

            records = tuple(weather_data.values())

            insert_script = '''INSERT INTO weather_data (last_updated_epoch, last_updated, temp_c, temp_f, is_day, condition, 
                                                         wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in, 
                                                         precip_mm, precip_in, humidity, cloud, feelslike_c, feelslike_f, 
                                                         vis_km, vis_miles, uv, gust_mph, gust_kph) 
                                                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                                                 %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
            cur.execute(insert_script, records)

    weather_data = get_api_data()
    upload_to_psql(weather_data=weather_data)

executing_dag = weather_to_psql()
