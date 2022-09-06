import inspect
import pandas as pd
from collections.abc import Iterable
from sqlalchemy import create_engine
from dagster_helpers import get_calling_function_name


def normalize_column(s):
        for a_remp, remp in {' ':'_', "'":'_', 'é':'e', 'è':'e', 'à':'a', '(':'_',')':'_','N°':'num'}.items():
            s = s.replace(a_remp, remp)
        return s.lower()

def get_to_postgres_function(enginedef, table_prefix):
    def to_postgres(df, asset_name=None):
        df.columns = [normalize_column(c) for c in df.columns]
        asset_name = asset_name or get_calling_function_name()
        asset_name = asset_name.replace('_postgres','')
        engine = create_engine(enginedef) 
        print('save to postgres asset : ', asset_name.lower())
        df.to_sql(table_prefix + asset_name.lower(), engine, if_exists='replace')
    return to_postgres

def get_to_bigquery_function(credentials_file, project_id, dataset):
    def to_bigquery(df, asset_name=None):
        # normalise les noms de colonne pour bigquery
        df.columns = [normalize_column(c) for c in df.columns]

        #envoie les données
        asset_name = asset_name or get_calling_function_name()
        asset_name = asset_name.replace('_bigquery','')
        from google.oauth2 import service_account
        credentials = service_account.Credentials.from_service_account_file(credentials_file)
        print('save to bigquery asset : ', asset_name.lower())
        df.to_gbq(project_id=project_id ,destination_table=f'{dataset}.{asset_name.lower()}', credentials=credentials , if_exists='replace')
    return to_bigquery
