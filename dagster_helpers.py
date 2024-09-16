import inspect, os
import pandas as pd
from dagster import asset, AssetObservation, AssetMaterialization, AssetKey, define_asset_job, repository, ScheduleDefinition, MetadataValue,\
    IOManager, graph, io_manager, op
from tabulate import tabulate
from collections.abc import Iterable
from typing import Union


#------------------------------------------------------------
#----------------------------- load value asset
#------------------------------------------------------------
from dags.repos import defs
defs.load_asset_value('test1_asset', partition_key="2024-09-14")


#------------------------------------------------------------
#----------------------------- divers
#------------------------------------------------------------
# previeuw markdown
df.head().to_markdown()


return MaterializeResult(
        metadata={  "nb_rows": int(len(df)),
                    "columns": MetadataValue.text(str(df.columns)),
                    "preview": MetadataValue.md(df.head().to_markdown()),
                    "types":MetadataValue.text(str(df['type'].unique())),
                    "types_de_paiement":MetadataValue.text(str(df['type_de_paiement'].unique()))
                }
    )


#------------------------------------------------------------
#----------------------------- partitions + assets checks + metadata
#------------------------------------------------------------
import json, os, requests
from dagster import asset, DailyPartitionsDefinition, Definitions, with_source_code_references, define_asset_job, AssetSelection, build_schedule_from_partitioned_job, AssetSpec, external_asset_from_spec
from dagster import MaterializeResult, AssetCheckResult, AssetCheckSpec, MetadataValue, multi_asset_check, Output

# ---------
daily_partition_def = DailyPartitionsDefinition(start_date="2024-01-01", end_offset=1)


################## Méthode 1 : check dans l'asset
@asset(group_name='tests', partitions_def=daily_partition_def,
       check_specs=[AssetCheckSpec("test_interne", asset="test2_asset")])
def test2_asset(context):
    
    partition_day = context.partition_key

    # a random number between 0 and 100
    import random
    a = str(random.randint(0, 100))
    
    context.add_output_metadata({"partition_day": partition_day}, output_name='result')
    context.add_output_metadata({"partition_day2": partition_day}, output_name='result')
    context.add_output_metadata({"partition_day3": partition_day}, output_name='result')
    #context.add_output_metadata({"metacheck_interne": 4}, output_name='test2_asset_test_interne') #devrait fonctionner mais ne fonctionne pas

    yield Output(value="test2 "+partition_day + a, 
                 metadata={"partition_day": partition_day})
    
    yield AssetCheckResult(passed=True, asset_key="test2_asset", check_name="test_interne", metadata={'truc':5})


################## Méthode 2 : check à l'extérieur de l'asset
@asset(group_name='tests', partitions_def=daily_partition_def)
def test1_asset(context):
    
    partition_day = context.partition_key

    context.add_output_metadata({"partition_day": partition_day})

    # a random number between 0 and 100
    import random
    a = str(random.randint(0, 100))

    return "test1 "+partition_day + a

@multi_asset_check(
    specs=[
        AssetCheckSpec("enough_rows", asset="test1_asset"),
        AssetCheckSpec("no_dupes", asset="test1_asset")
    ],
)
def checks(context):
    partition_key = context.run.tags["dagster/partition"]
    context.log.info("checking " + partition_key)
    from dags.repos import defs
    value = defs.load_asset_value('test1_asset', partition_key=partition_key)
    yield AssetCheckResult(passed=True, asset_key="test1_asset", check_name="enough_rows", metadata={'value':value,
                                                                                                     'run_config':context.run.run_config,
                                                                                                     'asset_selection':context.run.asset_selection})
    yield AssetCheckResult(passed=False, asset_key="test1_asset", check_name="no_dupes", metadata={'coucou':'coucoucou'})

#----------- definitions
defs = Definitions(
    assets= [test1_asset, test2_asset],
    asset_checks=[checks]
)

#------------------------------------------------------------
#----------------------------- local parquet io manager et autres trucs
#------------------------------------------------------------

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
       class LocalParquetIOManager(IOManager):
        def _get_path(self, context) -> str:
            """Automatically construct filepath."""
            directory = '/workspace/gitignore_data/'
            ide = context.get_asset_identifier()
            if context.has_partition_key:
                directory = directory + ide[0]
            if not os.path.exists(directory):
                os.makedirs(directory)
            fullpath = os.path.join(directory, ('_'.join(ide)))  + '.parquet'
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
                    metadata[key] = int(value)
                if ('float64') in str(type(value)):
                    metadata[key] = float(value)

            context.add_output_metadata(metadata)
            df.to_parquet(self._get_path(context))

        def load_input(self, context):
            return pd.read_parquet(self._get_path(context.upstream_output))
    return LocalParquetIOManager()
