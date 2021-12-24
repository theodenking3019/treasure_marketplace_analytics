# This script pulls raw txs from the ArbiScan API, uploads them to s3,
# and writes the resutls to relevant SQL tables.
# We'll use it to automate db refreshes using AWS Lambda.

import datetime as dt
import io
import json
import pytz
import re
import requests
import os

import boto3
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from ratelimit import limits, sleep_and_retry

ONE_SECOND = 1
DAO_WALLET = '0xdb6ab450178babcf0e467c1f3b436050d907e233'
os.chdir('v2_mysql/build_database_test')
DAO_ROYALTY_PCT = 0.05

tz = pytz.timezone('UTC')

# read in credentials
api_key_path = os.path.join("constants", "api_key.txt")
with open(api_key_path) as key_file:
    arbiscan_api_key = key_file.read()

sql_credential = os.path.join("constants", "mysql_credential.json")
with open(sql_credential) as f:
    mysql_credentials = json.loads(f.read())

# read in important contract addresses
contract_address = os.path.join("constants", "contract_addresses.json")
method_id_address = os.path.join("constants", "marketplace_method_ids.json")
treasure_id_address = os.path.join("constants", "treasure_token_ids.json")
with open(contract_address) as contract_address_file:
    contract_addresses = json.loads(contract_address_file.read())
with open(method_id_address) as ids_file:
    method_ids = json.loads(ids_file.read())
with open(treasure_id_address) as ids_file:
    treasure_ids = json.loads(ids_file.read())

contract_addresses_reverse = dict((v,k) for k,v in contract_addresses.items())

contract_addresses_reverse_lower = {}
for key in contract_addresses_reverse.keys():
    contract_addresses_reverse_lower[key.lower()] = contract_addresses_reverse[key]

treasure_ids_numeric = {}
for key in treasure_ids.keys():
    treasure_ids_numeric[int(key)] = treasure_ids[key]

# Get latest transactions from s3
s3_resource = boto3.resource('s3')
get_last_modified = lambda obj: int(obj.last_modified.strftime('%s'))
bucket = s3_resource.Bucket("treasure-marketplace-db")
objs = [obj for obj in bucket.objects.all() if obj.key[:16]=='marketplace-txs/']
objs = [obj for obj in sorted(objs, key=get_last_modified)]
latest_mkt_txs = pd.read_csv(io.BytesIO(objs[-1].get()["Body"].read()))
latest_block = latest_mkt_txs.blockNumber.max()
latest_txs = latest_mkt_txs.hash

# define functions needed for final script
@sleep_and_retry
@limits(calls=2, period=ONE_SECOND)
def get_contract_transactions(arbiscan_api_key, contract_address, start_block=0, from_address=None, tx_type="txlist"):

    request_url = "https://api.arbiscan.io/api"
    request_url = request_url + "?module=account"
    request_url = request_url + "&action=" + tx_type
    request_url = request_url + "&address=" + (contract_address if from_address is None else from_address)
    if tx_type != "txlist":
        request_url = request_url + "&contractaddress=" + contract_address
    request_url = request_url + "&startblock=" + str(start_block)
    request_url = request_url + "&endblock=99999999"
    request_url = request_url + "&sort=desc"
    request_url = request_url + "&apikey=" + arbiscan_api_key

    response = requests.get(request_url)
    return response.json()

def pull_arbiscan_data(arbiscan_api_key, method_ids, start_block=0, latest_tx_hashes=[]):
    # read in marketplace txs
    marketplace_txs = get_contract_transactions(arbiscan_api_key, contract_addresses['treasure_marketplace'], start_block=start_block)
    marketplace_txs_df = pd.DataFrame.from_dict(marketplace_txs["result"])

    # filter txs against already existing records
    marketplace_txs_df = marketplace_txs_df.loc[~marketplace_txs_df['hash'].isin(latest_tx_hashes)]

    # keep only successful txs
    marketplace_txs_df = marketplace_txs_df.loc[marketplace_txs_df.txreceipt_status=='1'].copy()

    # setup to pull magic txs - filter marketplace txs down to buys
    marketplace_txs_df["tx_type"] = [x[:10] for x in marketplace_txs_df["input"]]
    marketplace_txs_df["tx_type"] = marketplace_txs_df["tx_type"].map(method_ids)
    marketplace_buys_df = marketplace_txs_df.loc[marketplace_txs_df["tx_type"]=="buyItem"].copy()
    marketplace_txs_df.drop("tx_type", axis=1, inplace=True)

    # pull magic txs
    new_magic_txs = []
    for i, tx_wallet in enumerate(marketplace_buys_df["from"].unique()):
        magic_txs = get_contract_transactions(arbiscan_api_key, contract_addresses['magic'], start_block=start_block, from_address=tx_wallet, tx_type="tokentx")
        magic_txs_df = pd.DataFrame.from_dict(magic_txs["result"])
        magic_txs_df = magic_txs_df.loc[~magic_txs_df['hash'].isin(latest_mkt_txs)]
        new_magic_txs.append(magic_txs_df)
    new_magic_txs_df = pd.concat(new_magic_txs)
    new_magic_txs_df = new_magic_txs_df.drop_duplicates(["hash", "value"])

    return marketplace_txs_df, new_magic_txs_df

def build_marketplace_tx_table(raw_marketplace_tx_table, raw_magic_tx_table, contract_addresses, method_ids):
    raw_marketplace_tx_table["tx_type"] = [x[:10] for x in raw_marketplace_tx_table["input"]]
    raw_marketplace_tx_table["tx_type"] = raw_marketplace_tx_table["tx_type"].map(method_ids)
    raw_marketplace_tx_table = raw_marketplace_tx_table.loc[raw_marketplace_tx_table["tx_type"]=="buyItem"]

    raw_marketplace_tx_table['timestamp'] = [dt.datetime.fromtimestamp(int(x), tz) for x in raw_marketplace_tx_table['timeStamp']]
    raw_marketplace_tx_table['timestamp'] = raw_marketplace_tx_table['timestamp'].astype(str)
    raw_marketplace_tx_table['gas_fee_eth'] = raw_marketplace_tx_table['gasPrice'].astype('int64') * 1e-9 * raw_marketplace_tx_table['gasUsed'].astype(int) * 1e-9 
    raw_marketplace_tx_table['nft_collection'] = [contract_addresses[x[33:74]] for x in raw_marketplace_tx_table['input']]
    raw_marketplace_tx_table['nft_id'] = [int(x[133:138], 16) for x in raw_marketplace_tx_table['input']]
    raw_marketplace_tx_table.loc[raw_marketplace_tx_table['nft_collection'].isin(['treasures', 'legions', 'legions_genesis']), 'nft_name'] = \
        raw_marketplace_tx_table.loc[raw_marketplace_tx_table['nft_collection'].isin(['treasures', 'legions', 'legions_genesis']), 'nft_id'].map(treasure_ids_numeric)
    raw_marketplace_tx_table.loc[~pd.isnull(raw_marketplace_tx_table['nft_name']),'nft_subcategory'] = \
        [re.sub(r'[0-9]+', '', x).rstrip() for x in raw_marketplace_tx_table.loc[~pd.isnull(raw_marketplace_tx_table['nft_name']),'nft_name']]
    raw_marketplace_tx_table['quantity'] = [int(x[262:266], 16) for x in raw_marketplace_tx_table['input']]

    # join magic txs table to get transaction values
    raw_magic_tx_table['value'] = raw_magic_tx_table['value'].astype("float64") * 1e-18
    mkt_magic_merged_table = raw_magic_tx_table.merge(raw_marketplace_tx_table, how='inner', on='hash')
    mkt_magic_merged_table = mkt_magic_merged_table.groupby('hash',as_index=False).agg({'value_x':["min", "max", "sum"]})
    mkt_magic_merged_table.columns = mkt_magic_merged_table.columns.droplevel()
    mkt_magic_merged_table.rename(columns={'':'hash','min':'dao_amt_received_magic', 'max':'seller_amt_received_magic', 'sum':'sale_amt_magic'}, inplace=True)
    # for sales where we only have one of the txs, assume it is the seller amount received
    # TODO: make this more intelligent by using the actual dao wallet address
    mkt_magic_merged_table.loc[mkt_magic_merged_table['dao_amt_received_magic']==mkt_magic_merged_table['seller_amt_received_magic'], 'sale_amt_magic'] = \
        mkt_magic_merged_table.loc[mkt_magic_merged_table['dao_amt_received_magic']==mkt_magic_merged_table['seller_amt_received_magic'], 'seller_amt_received_magic'] / 0.95
    mkt_magic_merged_table.loc[mkt_magic_merged_table['dao_amt_received_magic']==mkt_magic_merged_table['seller_amt_received_magic'], 'dao_amt_received_magic'] = \
        mkt_magic_merged_table.loc[mkt_magic_merged_table['dao_amt_received_magic']==mkt_magic_merged_table['seller_amt_received_magic'], 'sale_amt_magic'] * 0.05

    raw_marketplace_tx_table = raw_marketplace_tx_table.merge(mkt_magic_merged_table, how='inner', on='hash')
    raw_marketplace_tx_table.drop('to', axis=1, inplace=True)
    to_wallets = raw_magic_tx_table.loc[raw_magic_tx_table['to']!=DAO_WALLET,['hash','to']].drop_duplicates('hash')
    raw_marketplace_tx_table = raw_marketplace_tx_table.merge(to_wallets, how='inner', on='hash')
    raw_marketplace_tx_table.rename(columns={
        'hash':'tx_hash',
        'timestamp':'datetime',
        'to':'wallet_seller',
        'from':'wallet_buyer'
        },inplace=True)

    # load into the table
    columns_to_load = [
        'tx_hash',
        'datetime',
        'wallet_buyer',
        'wallet_seller',
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
    return raw_marketplace_tx_table.loc[:, columns_to_load]

def refresh_database(sql_credentials):
    engine = create_engine(
        "mysql+pymysql://{user}:{pw}@{host}/{db}".format(
        user=sql_credentials['username'], 
        pw=sql_credentials['pw'], 
        host=sql_credentials['host'], 
        db="treasure"
        )
    )
    connection = engine.connect()

    marketplace_df, magic_df = pull_arbiscan_data(arbiscan_api_key, method_ids, latest_block, latest_txs)
    marketplace_sales_df = build_marketplace_tx_table(marketplace_df, magic_df, contract_addresses_reverse_lower, method_ids)

    # write data to s3
    date = dt.datetime.now()
    marketplace_txs_filename = f'marketplace-txs/marketplace_txs_raw_{date.year}_{date.month}_{date.day}_{date.hour}_{date.minute}.csv'
    magic_txs_filename = f'magic-txs/magic_txs_raw_{date.year}_{date.month}_{date.day}_{date.hour}_{date.minute}.csv'
    marketplace_df.to_csv('/tmp/tmp_marketplace_txs_df.csv')
    magic_df.to_csv('/tmp/tmp_magic_txs_df.csv')
    s3_resource.Object('treasure-marketplace-db', marketplace_txs_filename).upload_file('/tmp/tmp_marketplace_txs_df.csv')
    s3_resource.Object('treasure-marketplace-db', magic_txs_filename).upload_file('/tmp/tmp_magic_txs_df.csv')
    os.remove('/tmp/tmp_marketplace_txs_df.csv')
    os.remove('/tmp/tmp_magic_txs_df.csv')

    # write data to sql
    marketplace_sales_df.to_sql(
        'marketplace_sales', 
        con = connection, 
        if_exists = 'append', 
        chunksize = 1000,
        index=False
        )

    engine.dispose()

refresh_database(mysql_credentials)
# pull_arbiscan_data(arbiscan_api_key, method_ids, latest_block, latest_txs)