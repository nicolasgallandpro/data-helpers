from datetime import date,datetime
import datetime as dt
from dateutil.relativedelta import *

#--------------------------------
#----- Non pandas, system
#--------------------------------
# import de custom modules qui fonctionnent meme sous dagster, et reload systématique
import sys, importlib
sys.path.append('/workspace/TETU/mailchimp_sync')
import functions
importlib.reload(functions)

# liste tous les fichiers d'un dossier, list all files of a directory
from os import listdir
from os.path import isfile, join
all_files = lambda path:[f for f in listdir(path) if isfile(join(path, f))]

#--------------------------------
#----- Special
#--------------------------------
pd.eval('RMSE = df.MSE ** 0.5', target=df)

s1 = pd.Series([1, 2, 3, np.nan, 5])
s1.interpolate()

#--------------------------------
#----- encoding dingueries
#--------------------------------
df2 = pd.read_csv("file.txt", sep='\t', encoding='windows-1252')
df2 = pd.read_csv("file.txt", sep='\t', encoding='utf-8')

#--------------------------------
#----- Dates
#--------------------------------

pd.Timestamp('NaT') #not a time

#!!! magic :
def parse_dates(df, cols):
    for col in cols:
        df[col] = pd.to_datetime(df[col], infer_datetime_format=True) 

# pas magic
mydateparser = lambda x: pd.datetime.strptime(x, "%d/%m/%Y %H:%M")
df = pd.read_csv(file, sep='\t', parse_dates = ['DATE_DEBUT'], date_parser=mydateparser)

normdate = lambda c: (c.str.split(' ',expand=True) )[0]
normdate2 = lambda c: c.dt.strftime('%Y/%m/%d')

df["mois_fin"] = df['date_fin'].dt.to_period('M') # to month
df["mois_fin"] = pd.to_datetime(df['date_fin'],format='%Y-%m-%d').dt.to_period('M') #parfois ne marche pas !!??
#autre solution (meilleure ?):
df['date_fin'].dt.floor('d') - pd.offsets.MonthBegin(n=1)

today = date.today()
def parse_date(datestr):
    return dt.datetime.strptime(datestr, '%Y-%m-%d').date()
def diff_month(d1, d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month
def plus_1_month(d):
    return d+relativedelta(months=+1)
def premier_jour_mois(d):
    return d.replace(day=1) # TODO: pb avec les heures ?
def dernier_jour_mois(d):
    return (d.replace(day=1)+relativedelta(months=+1))-dt.timedelta(days=1)
def dernier_jour_mois_suivant(d):
    return dernier_jour_mois(plus_1_month(d))
def dernier_jour_mois_precedent(d):
    return d.replace(day=1)-dt.timedelta(days=1)
fix_month = lambda c: c.strftime('%Y-%m') #voir ligne en dessous
customersXmonth_enrich[START_MONTH] = customersXmonth_enrich[START_MONTH].dt.strftime('%Y-%m')

def quarter(month): #trimestre
    year, m = month.split('-')
    m = int(m)
    q = "Q1" if m<4 else "Q2" if m<7 else "Q3" if m<10 else "Q4"
    return year + '-' + q

#--------------------------------
#----- Zeppelin
#--------------------------------
z.show
sqlContext.createDataFrame(df).registerTempTable("raw")

%sql 
select * 
from raw 
where numero_client = ${id_client=243333} 
order by date_debut

#--------------------------------
#----- imports exports 
#--------------------------------
#parquet
df.to_parquet("...",  compression='gzip')
df = pd.read_parquet("...")

#sql
from sqlalchemy import create_engine
orders_exp = orders_all.copy()
orders_exp['order_date']= orders_exp['order_date'].dt.strftime('%Y-%m')
orders_exp['order__id'] = orders_exp['order_id']
orders_exp.drop(columns=['order_id'], inplace = True)
orders_exp.reset_index(inplace=True)
orders_exp.to_sql('Orders_all', engine, if_exists="replace")

#--------------------------------
#----- infos sur le dataframe
#--------------------------------
#def nunique(df, col):
#    return df.fillna('NAN').groupby(col).agg({df.columns[0]: 'count'})
#nunique(df,'origine_abonnement').sort_values(by='code_client').reset_index().to_numpy()
#===> à la place ::
df.column.value_counts() #normalize=True (%tage)   bins=3 (pour les catégories)   dropna=False/True

# création de "classes" de valeurs: 
df.column.value_counts(bins=3)

#qcut
pd.qcut(df['ext price'], q=10, precision=0) # !!!!! QCUT !!!!
#Quantile-based discretization function.
#Discretize variable into equal-sized buckets based on rank or based on sample quantiles. For example 1000 values for 10 quantiles would produce a Categorical object indicating quantile membership for each data point.

#cut
cut_labels_4 = ['silver', 'gold', 'platinum', 'diamond']
cut_bins = [0, 70000, 100000, 130000, 200000]
df['cut_ex1'] = pd.cut(df['ext price'], bins=cut_bins, labels=cut_labels_4)
#The major distinction is that qcut will calculate the size of each bin in order to make sure the distribution of data in the bins is equal. 
#In other words, all bins will have (roughly) the same number of observations but the bin range will vary.

On the other hand, cut is used to specifically define the bin edges. There is no guarantee about the distribution of items in each bin

df.info()

df.shape[0]     #rows

#--------------------------------
#----- filtres  |  sort/tri   | duplicates
#--------------------------------
df = df[~ df.code_selection.isin(selection_code_to_drop)]

df.query("date_fin > date_debut", inplace=True) 

free_a_virer = (df['method'] == "free")  &  ( df['sku'] != 'sku1' )
df = df[~ free_a_virer ]

df2 = df.drop_duplicates(subset=['order_id','sku'])

df.sort_values(by=['numero_client', 'date_debut'], inplace=True)

#--------------------------------
#----- filtres functions
#--------------------------------
def filtr(df, func, comment):
    rows_before = df.shape[0]
    df2 = func(df)
    removed =  (rows_before - df2.shape[0])
    print(comment," : ",removed, 'rows removed,  = ', str(round(removed * 100 / rows_before, 2)), '\%')
    return df2 
    
def keep_on_query(df, query, comment):
    return filtr(df, lambda x: x.query(query), comment)

def remove_where_empty_col(df, col):
    return filtr(df, lambda df : df.dropna(subset=[col]), col + ' vide')

#--------------------------------
#------ operations sur plusieurs df
#--------------------------------
 pd.concat([...])

#--------------------------------
#------ apply
#--------------------------------
df.apply(rowfunc, axis=1)

#--------------------------------
#------ types 
#--------------------------------
dfp = dfp.astype({"numero_client":"int64"})

#--------------------------------
#------ groupby
#--------------------------------
grouped = df.groupby(df.Name)
Tanya = grouped.get_group("Tanya")
for name, group in grouped:
    ....

#découpe par valeur de colonne
def onegroup(df):
    z.show( df)
    return df
out2 = test.groupby("numero_client").apply(onegroup)
z.show(out2)

#agregateurs
concat_unique = lambda c: ",".join(str(v) for v in set(c))
concat = lambda c: ",".join(str(v) for v in c)
concat_sans_none = lambda c: ",".join(str(v) for v in filter(lambda x:x!=None, c))
def check_same(serie):
    if serie.size == 0:
        return None
    s = set(serie)
    if len(s) >1:
        return "incoherent"
        #raise Exception("check_same a échoué", s, serie.size, serie)
    return s.pop()

#ex1
temp = orders_90.groupby('customer_id')
cus = temp.agg({
    'customer_id':'last',
    'sku': concat_unique,
    'type': concat,
    'order_date': concat
})

#--------------------------------
#------ merge, join innerjoin ...
#--------------------------------
CA = pd.merge(CA,acquisition,on='first_month',how='left')

#remplace les na par la valeur d'une autre colonne (fait une sorte de OU logique entre 2 colonnes):
final.email = final.email.combine_first(final['ID Utilisateur'])


#--------------------------------
#------ change value on condition (max, substring, ...)
#--------------------------------
df.loc[df['type_abo'].str.contains("365"), "facturation"] = "Annuel"
df['a'].where(df['a'] <= maxVal, maxVal) 

#--------------------------------
#------ graphiques
#--------------------------------
df.plot(figsize=(12,8))
