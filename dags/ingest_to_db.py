import requests
import os
import pandas as pd
from sqlalchemy import create_engine
import pendulum, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    'ingest_to_db',
    default_args={"retries":2},
    schedule=None,
    start_date=pendulum.datetime(2023, 3, 10)
) as dag:
    def extract_data(token, days):
        headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
        }

        yesterday = int((datetime.datetime.now() - datetime.timedelta(days=days)).timestamp()) * 1000
        request = requests.get(f'https://api.spotify.com/v1/me/player/recently-played?before={yesterday}&limit=50', headers=headers)
        
        return request.json()
    
    def transform_data(data):
        songs = []
        artists = []
        time_played = []
        dates = []

        for item in data['items']:
            songs.append(item['track']['name'])

            if len(item['track']['album']['artists']) == 1:
                artists.append(item['track']['album']['artists'][0]['name'])
            else:
                temp = []
                for a in item['track']['album']['artists']:
                    temp.append(a['name'])
                artists.append(" & ".join(t for t in temp))
            
            time_played.append(item['played_at'].split('T')[1].split('.')[0])
            dates.append(item['played_at'].split('T')[0])

        data_dict = {
            "songs": songs,
            "artists": artists,
            "time_played": time_played,
            "dates": dates
        }

        return pd.DataFrame(data_dict)

    def load_data(data):
        user = 'airflow'
        password = 'airflow'
        host = 'postgres'
        port = '5432'
        db = 'airflow'
        table_name = 'spotify_recently_played'

        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

        data.to_sql(name=table_name, con=engine, if_exists='replace')
    
    def etl():
        token = 'BQDPeAT4txrCVkCae6bNv2NKmb711LG8arvWV9H9eQd8QN581Ci7WVrYyO-H3RrkFT738C1Zm2N3-mwpMfgQNsD1HxK7NJtu9FLOBhzPOgHq9FM_OXohdonMgXQWYcNJxfTI79Vg73iest62wnTHLuDcHnTDnwDsH2Rl-zf9SNxxASkOqIICQpiy'
        data = extract_data(token, 1)
        data = transform_data(data)
        load_data(data)

    task = PythonOperator(
        task_id='etl',
        python_callable=etl,
    )

    task