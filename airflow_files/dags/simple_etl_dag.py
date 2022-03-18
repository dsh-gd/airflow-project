from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.filesystem import FSHook

with DAG(dag_id="simple_etl_dag", schedule_interval=None, default_args={"start_date": datetime(2021, 9, 1)}) as dag:

    @task
    def download_dataset(filename: str = "data.csv") -> Path:
        url = "https://data.bloomington.in.gov/dataset/117733fb-31cb-480a-8b30-fbf425a690cd/resource/8673744e-53f2-42d1-9d05-4e412bd55c94/download/monroe-county-crash-data2003-to-2015.csv"

        base_path = FSHook(conn_id="fs_default").get_path()
        file_path = Path(base_path, filename)

        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(file_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        return str(file_path)

    @task
    def count_number_of_accidents_per_year(file_path: str) -> str:
        df = pd.read_csv(file_path, encoding="ISO-8859-1")
        res = df.groupby("Year").size()
        return res.to_string(header=False)

    @task
    def print_results(number_of_accidents: str) -> None:
        for s in number_of_accidents.split("\n"):
            print(s)

    file_path = download_dataset()
    number_of_accidents = count_number_of_accidents_per_year(file_path)
    print_results(number_of_accidents)
