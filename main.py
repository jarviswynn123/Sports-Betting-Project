import pandas as pd
import os
import sys
import logging
import pandas as pd
from google.cloud import bigquery
from hashlib import md5
from typing import List
import shortuuid
import numpy
import pyarrow
import uuid
import numpy as np

# SETUP

DATA_DIR = "./spreadspoke_scores.csv"

PROJECT_NAME = "team-week-10"
DATASET_NAME = "nfl_sports_betting"

# TABLE SCHEMAS

TABLE_METADATA = {
    "point_spread": {
        "table_name": "fct_points_spread",
        "schema": [
            bigquery.SchemaField("uuid", "string", mode="REQUIRED"),
            bigquery.SchemaField("team_favorite_id", "string", mode="REQUIRED"),
            bigquery.SchemaField("team_home", "string", mode="REQUIRED"),
            bigquery.SchemaField("team_away", "string", mode="Required"),
            bigquery.SchemaField("spread_favorite", "float64", mode="Required"),
        ]
    },
    "over_under": {
        "table_name": "fct_over_under",
        "schema": [
            bigquery.SchemaField("uuid", "string", mode="REQUIRED"),
            bigquery.SchemaField("over_under_line", "float64", mode="NULLABLE"),
            bigquery.SchemaField("team_favorite_id", "string", mode="REQUIRED"),
            bigquery.SchemaField("team_home", "string", mode="REQUIRED"),
            bigquery.SchemaField("score_home", "int64", mode="REQUIRED"),
            bigquery.SchemaField("score_away", "int64", mode="REQUIRED"),
            bigquery.SchemaField("team_away", "string", mode="REQUIRED"),
            bigquery.SchemaField("total_score", "int64", mode="REQUIRED"),
            bigquery.SchemaField("over", "boolean", mode="REQUIRED"),
        ]
    },
    "games": {
        "table_name": "dim_games",
        "schema": [
            bigquery.SchemaField("date", "date", mode="REQUIRED"),
            bigquery.SchemaField("season", "int64", mode="REQUIRED"),
            bigquery.SchemaField("week", "string", mode="REQUIRED"),
            bigquery.SchemaField("playoff", "boolean", mode="REQUIRED"),
            bigquery.SchemaField("team_home", "string", mode="REQUIRED"),
            bigquery.SchemaField("score_home", "int64", mode="REQUIRED"),
            bigquery.SchemaField("score_away", "int64", mode="REQUIRED"),
            bigquery.SchemaField("team_away", "string", mode="REQUIRED"),
            bigquery.SchemaField("stadium", "string", mode="REQUIRED"),
            bigquery.SchemaField("temperature_F", "float64", mode="NULLABLE"),
            bigquery.SchemaField("wind_mph", "float64", mode="NULLABLE"),
            bigquery.SchemaField("humidity_%", "float64", mode="NULLABLE"),
            bigquery.SchemaField("uuid", "string", mode="REQUIRED")
        ]
    },
    "lookup_teams": {
        "table_name": "lookup_teams",
        "schema": [
            bigquery.SchemaField("team_id", "string", mode="REQUIRED"),
            bigquery.SchemaField("team_name", "string", mode="REQUIRED"),
            bigquery.SchemaField("conference_string", "string", mode="REQUIRED"),
            bigquery.SchemaField("division", "string", mode="REQUIRED"),
        ]
    }
}


# LOGGING SETUP
logging.basicConfig(
    format='[%(levelname)-5s][%(asctime)s][%(module)s:%(lineno)04d] : %(message)s',
    level=logging.INFO,
    stream=sys.stdout
)
logger: logging.Logger = logging.getLogger("root")
logger.setLevel(logging.DEBUG)



dataset_id = f"{PROJECT_NAME}.{DATASET_NAME}"
client = bigquery.Client()

def create_dataset(client: bigquery.Client, dataset_id: str, location: str = "US"):
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = location
    dataset = client.create_dataset(dataset, exists_ok=True)
    logger.info(f"Created dataset: {dataset.full_dataset_id}")

create_dataset(client, dataset_id)



df = pd.read_csv("./spreadspoke_scores.csv", header=0)

df = df[df["schedule_season"] > 1978]

df['uuid'] = [uuid.uuid4() for _ in range(len(df.index))]

df = df.astype({"uuid": "string"})

df.dropna(subset=["score_home", "score_away"], inplace=True)

df = df.reset_index(drop=True)

df["over_under_line"] = pd.to_numeric(df["over_under_line"], errors="coerce")




games_df = df.drop(columns=["weather_detail", "stadium_neutral", "over_under_line", "spread_favorite", "team_favorite_id"])

games_df = games_df.rename(columns={"weather_humidity": "humidity_%", "weather_wind_mph": "wind_mph", "weather_temperature": "temperature_F", "schedule_date": "date", "schedule_season": "season", "schedule_week": "week", "schedule_playoff": "playoff"})

games_df["date"] = pd.to_datetime(games_df["date"])

games_df = games_df.astype({"score_home": int, "score_away": int, "temperature_F": int, "wind_mph": int, "humidity_%": int}, errors="ignore")



games_table_name = f"{PROJECT_NAME}.{DATASET_NAME}.{TABLE_METADATA['games']['table_name']}"
games_schema = schema=TABLE_METADATA['games']['schema']

def load_table(
    df: pd.DataFrame, 
    client: bigquery.Client, 
    table_name: str, 
    schema: List[bigquery.SchemaField], 
    create_disposition: str = 'CREATE_IF_NEEDED', 
    write_disposition: str = 'WRITE_TRUNCATE'
    ) -> None:
    """load dataframe into bigquery table

    Args:
        df (pd.DataFrame): dataframe to load
        client (bigquery.Client): bigquery client
        table_name (str): full table name including project and dataset id
        schema (List[bigquery.SchemaField]): table schema with data types
        create_disposition (str, optional): create table disposition. Defaults to 'CREATE_IF_NEEDED'.
        write_disposition (str, optional): overwrite table disposition. Defaults to 'WRITE_TRUNCATE'.
    """

    # test table name to be full table name including project and dataset name. It must contain to dots
    assert len(table_name.split('.')) == 3, f"Table name must be a full bigquery table name including project and dataset id: '{table_name}'"


    job_config = bigquery.LoadJobConfig(
        create_disposition=create_disposition,
        write_disposition=write_disposition,
        schema=schema
    )
    logger.info(f"loading table: '{table_name}'")
    job = client.load_table_from_dataframe(df, destination=table_name, job_config=job_config)
    job.result() 

    # get the resulting table
    table = client.get_table(table_name)
    logger.info(f"loaded {table.num_rows} rows into {table.full_table_id}")

load_table(games_df, client, games_table_name, games_schema)


client = bigquery.Client()
print(f"BigQuery Project: {client.project}")

# list datasets
print("Listing datasets:")

for dataset in client.list_datasets():
    dataset_id = dataset.dataset_id
    print(f"dataset id: `{dataset_id}`, full_name: `{dataset.full_dataset_id}`, labels (tags): {dataset.labels}")


    over_under_df = df[["uuid", "over_under_line", "team_favorite_id", "team_home", "score_home", "score_away", "team_away"]]
# display(over_under_df.head(3))

def total_score(df):
    for _ in range(len(df.index)):
        df["total_score"] = df["score_home"] + df["score_away"]
    return df
total_score(over_under_df)


over_under_df = over_under_df.astype({"score_home": int, "score_away": int, "over_under_line": float, "total_score": int}, errors="ignore")

over_under_df["over_under_line"] = pd.to_numeric(over_under_df["over_under_line"], errors="coerce")

for _ in range(len(over_under_df.index)):
    over_under_df["over"] = over_under_df["total_score"] > over_under_df["over_under_line"]
        # over_under_df["total_score"] == over_under_df["over_under_line"]
        
over_under_df = over_under_df.reset_index(drop=True)

over_under_table_name = f"{PROJECT_NAME}.{DATASET_NAME}.{TABLE_METADATA['over_under']['table_name']}"
over_under_schema = schema=TABLE_METADATA['over_under']['schema']

load_table(over_under_df, client, over_under_table_name, over_under_schema)


point_spread_df = df[["uuid", "team_home", "score_home", "score_away", "team_away", "team_favorite_id", "spread_favorite"]]

point_spread_df = point_spread_df.astype({"score_home": int, "score_away": int})


point_spread_table_name = f"{PROJECT_NAME}.{DATASET_NAME}.{TABLE_METADATA['point_spread']['table_name']}"
point_spread_schema = schema=TABLE_METADATA['point_spread']['schema']

load_table(point_spread_df, client, point_spread_table_name, point_spread_schema)