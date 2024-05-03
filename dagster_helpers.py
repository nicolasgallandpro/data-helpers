import inspect, os
import pandas as pd
from dagster import asset, AssetObservation, AssetMaterialization, AssetKey, define_asset_job, repository, ScheduleDefinition, MetadataValue,\
    IOManager, graph, io_manager, op
from tabulate import tabulate
from collections.abc import Iterable
from typing import Union


#The context object now has an asset_key property to get the AssetKey of the current asset.

#récupérer la valeur des assets
#from repos import defs
#defs.load_asset_value('offer_infos')

def markdown_describe(df):
    """A markdown description of a dataframe"""
    md = '### Rows\n'
    md += f"{len(df)} rows  \n"


    md += '### Memory usage\n'
    md += f"{df.memory_usage(deep=True).sum() / (1024*1024)} Mb  \n"

    if (len(df) == 0 or len(df.columns) == 0):
        return md

    md += '  \n'
     # stats sur les colonnes
    def most_common_value(col):
        try:
            value = list(df[col].value_counts().reset_index().values[0])
            return f'{value[0]}: {value[1]} occurences'
        except:
            print("colonne qui bug :", col)
            return '?'
    md += '\n  \n  '
    
    if len(df)<100 and len(df.columns) < 15:
        return md + '### Dataframe \n' + df.to_markdown()
    
    md += '### Head 5\n'
    md += df.head(5).to_markdown()
    md += '\n  \n  ' 
    md += '### Columns\n'
    stats = pd.DataFrame(
        [
            ['dtype']+[str(t) for t in df.dtypes],
            ['fill rate']+[  (str(round(100.* (~pd.isnull(df[col])).sum() / len(df[col]), 2)) + ' %')   for col in df.columns],
            ['most common value']+[  most_common_value(c)   for c in df.columns],
            ['uniques values']+[len(df[c].unique()) for c in df.columns],
            ['% uniques values']+[str(round(100. * len(df[c].unique()) / len(df),1))+'%' for c in df.columns]
        ], columns = ['__STAT__']+ list(df.columns)).set_index('__STAT__').transpose()
    md += stats.to_markdown()
    md += '\n  \n  ' 

    md += '### Numerical columns\n'
    md += df.describe().to_markdown()

    return md


#from repos import defs
#defs.load_asset_value('all_users_ids_abonnes')
def get_asset(asset_name):
    try:
        out = []
        for asset in asset_name:
            out.append(pd.read_pickle(f'/opt/dagster/dagster_home/storage/{asset}'))
        return out
    except:
        return pd.read_pickle(f'/opt/dagster/dagster_home/storage/{asset_name}')

#from repos import defs
#defs.load_asset_value('all_users_ids_abonnes')
def get_parquet_asset(asset_name):
    return pd.read_parquet('/workspace/gitignore_data/'\
                           +asset_name.replace(' ','')+'.parquet')



def deleted_rows_recorder(start):
    """Helper that make a dict recording rows deleted in every step """
    rec = {}
    supprs = []
    def record(txt=None, rows=None):
        if txt==None and rows==None:
            return rec
        rows = len(rows) if type(rows) == pd.DataFrame else rows
        sup = start - sum(supprs) - rows
        rec[txt] = sup
        supprs.append(sup)
        
        return rec
    return record


    
@io_manager
def local_parquet_io_manager():
    
    class LocalParquetIOManager(IOManager):
        def _get_path(self, context) -> str:
            """Automatically construct filepath."""
            if context.has_asset_key:
                path = context.get_asset_identifier()
            else:
                path = context.get_identifier()
            fullpath = os.path.join('/workspace/gitignore_data/', *path) + '.parquet'
            directory = os.path.dirname(fullpath)
            print(directory)
            if not os.path.exists(directory):
                os.makedirs(directory)
            return fullpath

        
        def handle_output(self, context, var):
            """handle the output of the asset (a dataframe or a dataframe and a metadata dict) """
            
            def _get_meta_and_df(var): #helper
                if type(var) == pd.DataFrame:
                    return {},var
                try:
                    if len(var) == 2:
                        var1, var2 = var
                        if type(var1) == type({}) and type(var2) == pd.DataFrame:
                            return var1, var2
                        elif type(var2) == type({}) and type(var1) == pd.DataFrame:
                            return var2,var1    
                    raise Exception("l'objet retourné au local_parquet_io_manager doit être un dataframe ou un iterable contenant un dictionnaire de meta et un dataframe")
                except e:
                    raise e
            
            metadata, df = _get_meta_and_df(var)
            metadata['Colonnes'] = MetadataValue.md(df.dtypes.to_markdown())
            metadata['Nb de lignes'] = len(df)
            metadata['Memory (Mb)'] = df.memory_usage(deep=True).sum() / (1024*1024)
            metadata['Describe'] = MetadataValue.md(markdown_describe(df))

            # fait la conversion vers des types que dagster reconnait
            for key, value in metadata.items():
                if ('int64') in str(type(value)):
                    observation[key] = int(value)

            context.add_output_metadata(metadata)
            df.to_parquet(self._get_path(context))

        def load_input(self, context):
            return pd.read_parquet(self._get_path(context.upstream_output))
    return LocalParquetIOManager()
