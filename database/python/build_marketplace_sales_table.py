# In this file we'll manipulate the data from the raw transactions table into something more
# human-readable.

# import packages
import datetime as dt
import json
import os
import pytz
import sqlite3
import re

import numpy as np
import pandas as pd

sqlite3.register_adapter(np.int64, lambda val: int(val))
sqlite3.register_adapter(np.int32, lambda val: int(val))
tz = pytz.timezone('UTC')

DAO_WALLET = '0xdb6ab450178babcf0e467c1f3b436050d907e233'
DAO_ROYALTY_PCT = 0.05

# read in important contract addresses, marketplace method IDs, and treausre IDs
contract_address = os.path.join("constants", "contract_addresses_reverse.json")
method_id_address = os.path.join("constants", "marketplace_method_ids.json")
treasure_id_address = os.path.join("constants", "treasure_token_ids.json")

with open(contract_address) as contract_address_file:
    contract_addresses = json.loads(contract_address_file.read())
with open(method_id_address) as ids_file:
    method_ids = json.loads(ids_file.read())
with open(treasure_id_address) as ids_file:
    treasure_ids = json.loads(ids_file.read())

contract_addresses_lower = {}
for key in contract_addresses.keys():
    contract_addresses_lower[key.lower()] = contract_addresses[key]

treasure_ids_numeric = {}
for key in treasure_ids.keys():
    treasure_ids_numeric[int(key)] = treasure_ids[key]

# create the marketplace_sales table if it doesn't exist
markeplace_sales_query_path = os.path.join('database','sql','create_markeplace_sales.sql')
connection = sqlite3.connect('treasure.db')
cursor = connection.cursor()
with open(markeplace_sales_query_path) as file:
    create_statement = file.read()
    cursor.execute(create_statement)

# Read in transactions and magic amounts tables
cursor.execute("SELECT * FROM marketplace_txs_raw;")
raw_marketplace_tx_table = cursor.fetchall()
raw_marketplace_tx_table = pd.DataFrame(raw_marketplace_tx_table)
raw_marketplace_tx_table.columns = [description[0] for description in cursor.description]

cursor.execute("SELECT * FROM magic_txs_raw;")
raw_magic_tx_table = cursor.fetchall()
raw_magic_tx_table = pd.DataFrame(raw_magic_tx_table)
raw_magic_tx_table.columns = [description[0] for description in cursor.description]

# filter marketplace tx table down to transactions not already accounted for
cursor.execute("SELECT * FROM marketplace_sales;")
current_marketplace_sales = cursor.fetchall()
if current_marketplace_sales!=[]:
    current_marketplace_sales = pd.DataFrame(current_marketplace_sales)
    current_marketplace_sales.columns = [description[0] for description in cursor.description]
    raw_marketplace_tx_table = raw_marketplace_tx_table.loc[~raw_marketplace_tx_table['hash'].isin(current_marketplace_sales['tx_hash'])]

# filter marketplace tx table down to sales and manipulate columns
raw_marketplace_tx_table["tx_type"] = [x[:10] for x in raw_marketplace_tx_table["input"]]
raw_marketplace_tx_table["tx_type"] = raw_marketplace_tx_table["tx_type"].map(method_ids)
raw_marketplace_tx_table = raw_marketplace_tx_table.loc[raw_marketplace_tx_table["tx_type"]=="buyItem"]

raw_marketplace_tx_table['timestamp'] = [dt.datetime.fromtimestamp(int(x), tz) for x in raw_marketplace_tx_table['timeStamp']]
raw_marketplace_tx_table['timestamp'] = raw_marketplace_tx_table['timestamp'].astype(str)
raw_marketplace_tx_table['gas_fee_eth'] = raw_marketplace_tx_table['gasPrice'] * 1e-9 * raw_marketplace_tx_table['gasUsed'] * 1e-9 
raw_marketplace_tx_table['nft_collection'] = [contract_addresses_lower[x[33:74]] for x in raw_marketplace_tx_table['input']]
raw_marketplace_tx_table['nft_id'] = [int(x[133:138], 16) for x in raw_marketplace_tx_table['input']]
raw_marketplace_tx_table.loc[raw_marketplace_tx_table['nft_collection'].isin(['treasures', 'legions', 'legions_genesis']), 'nft_name'] = \
    raw_marketplace_tx_table.loc[raw_marketplace_tx_table['nft_collection'].isin(['treasures', 'legions', 'legions_genesis']), 'nft_id'].map(treasure_ids_numeric)
raw_marketplace_tx_table.loc[~pd.isnull(raw_marketplace_tx_table['nft_name']),'nft_subcategory'] = \
    [re.sub(r'[0-9]+', '', x).rstrip() for x in raw_marketplace_tx_table.loc[~pd.isnull(raw_marketplace_tx_table['nft_name']),'nft_name']]
raw_marketplace_tx_table['quantity'] = [int(x[262:266], 16) for x in raw_marketplace_tx_table['input']]

# join magic txs table to get transaction values
raw_magic_tx_table['tx_value'] = raw_magic_tx_table['tx_value'] * 1e-18
mkt_magic_merged_table = raw_magic_tx_table.merge(raw_marketplace_tx_table, how='inner', on='hash')
mkt_magic_merged_table = mkt_magic_merged_table.groupby('hash',as_index=False).agg({'tx_value_x':["min", "max", "sum"]})
mkt_magic_merged_table.columns = mkt_magic_merged_table.columns.droplevel()
mkt_magic_merged_table.rename(columns={'':'hash','min':'dao_amt_received_magic', 'max':'seller_amt_received_magic', 'sum':'sale_amt_magic'}, inplace=True)
# for sales where we only have one of the txs, assume it is the seller amount received
# TODO: make this more intelligent by using the actual dao wallet address
mkt_magic_merged_table.loc[mkt_magic_merged_table['dao_amt_received_magic']==mkt_magic_merged_table['seller_amt_received_magic'], 'sale_amt_magic'] = \
    mkt_magic_merged_table.loc[mkt_magic_merged_table['dao_amt_received_magic']==mkt_magic_merged_table['seller_amt_received_magic'], 'seller_amt_received_magic'] / 0.95
mkt_magic_merged_table.loc[mkt_magic_merged_table['dao_amt_received_magic']==mkt_magic_merged_table['seller_amt_received_magic'], 'dao_amt_received_magic'] = \
    mkt_magic_merged_table.loc[mkt_magic_merged_table['dao_amt_received_magic']==mkt_magic_merged_table['seller_amt_received_magic'], 'sale_amt_magic'] * 0.05

raw_marketplace_tx_table = raw_marketplace_tx_table.merge(mkt_magic_merged_table, how='inner', on='hash')
raw_marketplace_tx_table.drop('to_wallet', axis=1, inplace=True)
to_wallets = raw_magic_tx_table.loc[raw_magic_tx_table['to_wallet']!=DAO_WALLET,['hash','to_wallet']].drop_duplicates('hash')
raw_marketplace_tx_table = raw_marketplace_tx_table.merge(to_wallets, how='inner', on='hash')


# load into the table
columns_to_load = [
    'hash',
    'timestamp',
    'to_wallet',
    'from_wallet',
    'sale_amt_magic',
    'seller_amt_received_magic',
    'dao_amt_received_magic',
    'gas_fee_eth',
    'nft_collection',
    'nft_id',
    'nft_name',
    'nft_subcategory',
    'quantity'
]
marketplace_sales_insert_records = raw_marketplace_tx_table.loc[:, columns_to_load].to_records(index=False)
cursor.executemany("INSERT INTO marketplace_sales VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", marketplace_sales_insert_records)
connection.commit()
connection.close()