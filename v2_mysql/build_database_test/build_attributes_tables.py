# In this script we'll read in the attributes for smolverse nfts
# and create tables in the db.
# Big thanks to andb0p for compiling the data!

import datetime as dt
import json
import pandas as pd
import os
from sqlalchemy import create_engine

os.chdir('v2_mysql/build_database_test')

# read in attributes
smol_brains_attributes_path = os.path.join("constants", "smol_brains_attributes.csv")
smol_bodies_attributes_path = os.path.join("constants", "smol_bodies_attributes.csv")
smol_cars_attributes_path = os.path.join("constants", "smol_cars_attributes.csv")
with open(smol_brains_attributes_path) as f:
    smol_brains_attributes = pd.read_csv(f)
with open(smol_bodies_attributes_path) as f:
    smol_bodies_attributes = pd.read_csv(f)
with open(smol_cars_attributes_path) as f:
    smol_cars_attributes = pd.read_csv(f)

smol_brains_attributes.head()
smol_bodies_attributes.head()
smol_cars_attributes.head()

# write to sql
sql_credential = os.path.join("constants", "mysql_credential.json")
with open(sql_credential) as f:
    mysql_credentials = json.loads(f.read())
engine = create_engine(
    "mysql+pymysql://{user}:{pw}@{host}/{db}".format(
    user=mysql_credentials['username'], 
    pw=mysql_credentials['pw'], 
    host=mysql_credentials['host'], 
    db="treasure"
    )
)
connection = engine.connect()

smol_brains_attributes.to_sql(
    'attributes_smol_brains', 
    con = connection, 
    if_exists = 'append', 
    chunksize = 1000,
    index=False
)
smol_bodies_attributes.to_sql(
    'attributes_smol_bodies', 
    con = connection, 
    if_exists = 'append', 
    chunksize = 1000,
    index=False
)
smol_cars_attributes.to_sql(
    'attributes_smol_cars', 
    con = connection, 
    if_exists = 'append', 
    chunksize = 1000,
    index=False
)

connection.close()
engine.dispose()