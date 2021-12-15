# In this script we will migrate the marketplace_sales table from our sqlite db
# to the new mysql db.

import csv
import json
import os
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import sqlite3

try:
    os.chdir("v2_mysql")
except:
    pass

# read in sqlite data
sqlite_connection = sqlite3.connect("../v1_sqlite/treasure.db")
sqlite_cursor = sqlite_connection.cursor()
sqlite_cursor.execute("SELECT * FROM marketplace_sales;")
marketplace_sales = sqlite_cursor.fetchall()
marketplace_sales = pd.DataFrame(marketplace_sales)
marketplace_sales.columns = [description[0] for description in sqlite_cursor.description]

marketplace_sales.rename(columns={'timestamp':'datetime'}, inplace=True)
sqlite_cursor.close()

# read in important contract addresses and marketplace method IDs
credential = os.path.join("constants", "mysql_credential.json")
with open(credential) as f:
    mysql_credentials = json.loads(f.read())


# write table to mysql, filtering out duplicate txs
engine = create_engine(
    "mysql+pymysql://{user}:{pw}@{host}/{db}".format(
        user=mysql_credentials['username'], 
        pw=mysql_credentials['pw'], 
        host=mysql_credentials['host'], 
        db="treasure"
        )
    )

connection = engine.connect()
existing_tx_hashes_result = connection.execute("SELECT tx_hash FROM marketplace_sales")

existing_tx_hashes_lst = []
for row in existing_tx_hashes_result:
     existing_tx_hashes_lst.append(row[0])

marketplace_sales = marketplace_sales.loc[~marketplace_sales.tx_hash.isin(existing_tx_hashes_lst)]

marketplace_sales.to_sql(
    'marketplace_sales', 
    con = engine, 
    if_exists = 'append', 
    chunksize = 1000,
    index=False
    )

engine.dispose()

