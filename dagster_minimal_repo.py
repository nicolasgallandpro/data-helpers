import requests, csv
from dagster import asset, AssetObservation, AssetMaterialization, AssetKey, define_asset_job, repository, ScheduleDefinition
import pandas as pd
from dagster_helpers import *


@asset(group_name="group1")
def cereals():
    response = requests.get("https://docs.dagster.io/assets/cereal.csv")
    lines = response.text.split("\n")
    return [row for row in csv.DictReader(lines)]


@asset
def nabisco_cereals(cereals):
    """Cereals manufactured by Nabisco"""
    return [row for row in cereals if row["mfr"] == "N"]

@asset(group_name='ideat_tgl', io_manager_key= local_parquet_io_manager)
def nabisco_cereals_df(nabisco_cereals):
    return pd.DataFrame(nabisco_cereals)


asset1_job = define_asset_job(name="asset1_job", selection=["nabisco_cereals", 'nabisco_cereals_df'])
job1_schedule = ScheduleDefinition(job=asset1_job, cron_schedule="0 0 * * *")

defs = Definitions(
    assets= ["nabisco_cereals", 'nabisco_cereals_df']s,
    #resources={"local_parquet_io_manager": local_parquet_io_manager},
    jobs=[asset1_job],
    schedules=[job1_schedule]
)
