# In this file we'll query Arbiscan to build the initial verison of the transactions database.
# This script only needs to be run once.
# Because Arbiscan limits queryable transactions to 10,000 at a time, running this script at a
# later date will not yield a complete database.

# import packages and set golbal vairables
import datetime as dt
import json
import requests
import os
import sqlite3

import pandas as pd
from ratelimit import limits, sleep_and_retry

import functions

# create the raw transaction tables if they don't exist
marketplace_txs_query_path = os.path.join('database','sql','create_marketplace_txs_raw.sql')
magic_txs_query_path = os.path.join('database','sql','create_magic_txs_raw.sql')

connection = sqlite3.connect('treasure.db')
cursor = connection.cursor()
with open(marketplace_txs_query_path) as file:
    create_statement = file.read()
    cursor.execute(create_statement)

with open(magic_txs_query_path) as file:
    create_statement = file.read()
    cursor.execute(create_statement)

# prep to call arbi api: read in api key
api_key_path = os.path.join("constants", "api_key.txt")
with open(api_key_path) as key_file:
    api_key = key_file.read()

# read in important contract addresses and marketplace method IDs
contract_address = os.path.join("constants", "contract_addresses.json")
method_id_address = os.path.join("constants", "marketplace_method_ids.json")
with open(contract_address) as contract_address_file:
    contract_addresses = json.loads(contract_address_file.read())
with open(method_id_address) as ids_file:
    method_ids = json.loads(ids_file.read())

# get txs on treasure markeplace
marketplace_txs = functions.get_contract_transactions(api_key, contract_addresses['treasure_marketplace'])
marketplace_txs_df = pd.DataFrame.from_dict(marketplace_txs["result"])

# filter txs against already existing records
cursor.execute("SELECT * FROM marketplace_txs_raw;")
current_marketplace_txs_raw = cursor.fetchall()
if current_marketplace_txs_raw!=[]:
    current_marketplace_txs_raw_df = pd.DataFrame(current_marketplace_txs_raw)
    current_marketplace_txs_raw_df.columns = [description[0] for description in cursor.description]
    marketplace_txs_df = marketplace_txs_df.loc[~marketplace_txs_df['hash'].isin(current_marketplace_txs_raw_df['tx_hash'])]

# setup to pull magic txs - pull current magic transactions to filter against
cursor.execute("SELECT * FROM magic_txs_raw;")
current_magic_tx_table = cursor.fetchall()
if current_magic_tx_table!=[]:
    current_magic_tx_table_df = pd.DataFrame(current_magic_tx_table)
    current_magic_tx_table_df.columns = [description[0] for description in cursor.description]

# setup to pull magic txs - filter marketplace txs down to buys
marketplace_txs_df["tx_type"] = [x[:10] for x in marketplace_txs_df["input"]]
marketplace_txs_df["tx_type"] = marketplace_txs_df["tx_type"].map(method_ids)
marketplace_buys_df = marketplace_txs_df.loc[marketplace_txs_df["tx_type"]=="buyItem"].copy()
marketplace_txs_df.drop("tx_type", axis=1, inplace=True)

# pull magic txs
new_magic_txs = []
for tx_wallet in marketplace_buys_df["from"].unique():
    magic_txs = functions.get_contract_transactions(api_key, contract_addresses['magic'], from_address=tx_wallet, tx_type="tokentx")
    try:
        magic_txs_df = pd.DataFrame.from_dict(magic_txs["result"])
    except:
        print(magic_txs["result"])
    if current_magic_tx_table!=[]:
        magic_txs_df = magic_txs_df.loc[~magic_txs_df['hash'].isin(current_magic_tx_table_df['hash'])]
    new_magic_txs.append(magic_txs_df)
new_magic_txs = pd.concat(new_magic_txs)
new_magic_txs = new_magic_txs.drop_duplicates(["hash", "value"])

# insert records 
marketplace_tx_insert_records = marketplace_txs_df.to_records(index=False)
magic_tx_insert_records = new_magic_txs.to_records(index=False)
cursor.executemany("INSERT INTO marketplace_txs_raw VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,? ,?);", marketplace_tx_insert_records)
cursor.executemany("INSERT INTO magic_txs_raw VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,? ,?, ?);", magic_tx_insert_records)
connection.commit()
connection.close()
