# This script wrangles our already existing ArbiScan data into
# a listings table. We can use this table to track floor prices.

import datetime as dt
import io
import json
import pytz
import re
import os

import boto3
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

os.chdir('v2_mysql/build_database_test')

tz = pytz.timezone('UTC')

# define functions
def get_contract_addresses():
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

    return contract_addresses_reverse_lower, method_ids, treasure_ids_numeric

def read_raw_marketplace_txs():
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket("treasure-marketplace-db")
    mkt_objs = [obj for obj in bucket.objects.all() if (obj.key[:16]=='marketplace-txs/') & (obj.key[-4:]=='.csv')]
    mkt_objs_lst = []
    for obj in mkt_objs:
        mkt_objs_lst.append(pd.read_csv(io.BytesIO(obj.get()["Body"].read())))

    marketplace_txs_raw = pd.concat(mkt_objs_lst)
    marketplace_txs_raw = marketplace_txs_raw.loc[marketplace_txs_raw.txreceipt_status==1].copy() # only keep successful txs
    return marketplace_txs_raw

def read_marketplace_sales():
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
    marketplace_sales_list = []
    marketplace_sales_query = connection.execute('SELECT * FROM treasure.marketplace_sales')
    for row in marketplace_sales_query:
        marketplace_sales_list.append(row)
    marketplace_sales = pd.DataFrame(marketplace_sales_list)
    marketplace_sales.columns=list(marketplace_sales_query.keys())
    connection.close()
    engine.dispose()

    return marketplace_sales

def process_raw_marketplace_txs(marketplace_txs_raw):
    marketplace_txs_raw["tx_type"] = [x[:10] for x in marketplace_txs_raw["input"]]
    marketplace_txs_raw["tx_type"] = marketplace_txs_raw["tx_type"].map(method_ids)
    marketplace_txs_raw = marketplace_txs_raw.loc[~pd.isnull(marketplace_txs_raw["tx_type"])] # null transactions are all whitelisting of certain accounts before marketplace launch
    marketplace_txs_raw['timestamp'] = [dt.datetime.fromtimestamp(int(x), tz) for x in marketplace_txs_raw['timeStamp']]
    marketplace_txs_raw['gas_fee_eth'] = (marketplace_txs_raw['gasPrice'].astype('int64') * 1e-9 * marketplace_txs_raw['gasUsed'].astype(int) * 1e-9) / 2
    marketplace_txs_raw['nft_collection'] = [contract_addresses_reverse_lower[x[33:74]] for x in marketplace_txs_raw['input']] # works for both types of txs
    marketplace_txs_raw['nft_id'] = [int(x[133:138], 16) for x in marketplace_txs_raw['input']] # also works for all types of txs

    marketplace_txs_raw.loc[marketplace_txs_raw['nft_collection'].isin(['treasures', 'legions', 'legions_genesis']), 'nft_name'] = \
        marketplace_txs_raw.loc[marketplace_txs_raw['nft_collection'].isin(['treasures', 'legions', 'legions_genesis']), 'nft_id'].map(treasure_ids_numeric)
    marketplace_txs_raw.loc[~pd.isnull(marketplace_txs_raw['nft_name']),'nft_subcategory'] = \
        [re.sub(r'[0-9]+', '', x).rstrip() for x in marketplace_txs_raw.loc[~pd.isnull(marketplace_txs_raw['nft_name']),'nft_name']]
    marketplace_txs_raw['quantity'] = np.nan
    marketplace_txs_raw['listing_price_magic'] = np.nan
    marketplace_txs_raw['expiration_datetime'] = np.nan
    marketplace_txs_raw.loc[marketplace_txs_raw['tx_type'].isin(['createListing','updateListing']), 'quantity'] = [int(x[198:202], 16) for x in marketplace_txs_raw.loc[marketplace_txs_raw['tx_type'].isin(['createListing','updateListing']), 'input']]
    marketplace_txs_raw.loc[marketplace_txs_raw['tx_type']=='buyItem', 'quantity'] = [int(x[262:266], 16) for x in marketplace_txs_raw.loc[marketplace_txs_raw['tx_type']=='buyItem', 'input']]
    marketplace_txs_raw.loc[marketplace_txs_raw['tx_type'].isin(['createListing','updateListing']), 'listing_price_magic'] = [int(x[240:266], 16) for x in marketplace_txs_raw.loc[marketplace_txs_raw['tx_type'].isin(['createListing','updateListing']), 'input']]
    marketplace_txs_raw.loc[marketplace_txs_raw['tx_type'].isin(['createListing','updateListing']), 'expiration_datetime'] = [int(x[310:330], 16) for x in marketplace_txs_raw.loc[marketplace_txs_raw['tx_type'].isin(['createListing','updateListing']), 'input']]
    marketplace_txs_raw.loc[marketplace_txs_raw['tx_type'].isin(['createListing','updateListing']), 'listing_price_magic'] = marketplace_txs_raw.loc[marketplace_txs_raw['tx_type'].isin(['createListing','updateListing']), 'listing_price_magic'].astype("float64") * 1e-18
    marketplace_txs_raw.loc[marketplace_txs_raw['tx_type'].isin(['createListing','updateListing']), 'expiration_datetime'] = [dt.datetime.fromtimestamp(int(x)/1000, tz) for x in marketplace_txs_raw.loc[marketplace_txs_raw['tx_type'].isin(['createListing','updateListing']), 'expiration_datetime']]

    # correct data error: coalesce from + from_wallet, to + to_wallet
    marketplace_txs_raw.loc[pd.isnull(marketplace_txs_raw['from_wallet']), 'from_wallet'] = marketplace_txs_raw.loc[pd.isnull(marketplace_txs_raw['from_wallet']), 'from']
    marketplace_txs_raw.loc[pd.isnull(marketplace_txs_raw['to_wallet']), 'to_wallet']  = marketplace_txs_raw.loc[pd.isnull(marketplace_txs_raw['to_wallet']), 'to']
    marketplace_txs_raw.rename(columns={
        'hash':'tx_hash',
        'timestamp':'datetime'
    }, inplace=True)
    columns_to_keep = [
        'tx_hash',
        'datetime',
        'blockNumber',
        'from_wallet',
        'to_wallet',
        'listing_price_magic',
        'expiration_datetime',
        'gas_fee_eth',
        'nft_collection',
        'nft_id',
        'nft_name',
        'nft_subcategory',
        'quantity',
        'tx_type'
    ]

    marketplace_txs_raw = marketplace_txs_raw.loc[:,columns_to_keep].copy()
    return marketplace_txs_raw

## Gather required data
# read in important contract addresses
contract_addresses_reverse_lower, method_ids, treasure_ids_numeric = get_contract_addresses()
# read in raw marketplace txs from s3 for listings and cancellations
marketplace_txs_raw = read_raw_marketplace_txs()
# read in existing marketplace_sales table for sales
sales = read_marketplace_sales()
sales = sales.merge(marketplace_txs_raw.loc[:,['hash','blockNumber']], left_on='tx_hash',right_on='hash')
# parse data
marketplace_txs_raw = process_raw_marketplace_txs(marketplace_txs_raw)

## Create listings table
listings_og = marketplace_txs_raw.loc[marketplace_txs_raw['tx_type'].isin(['createListing','updateListing'])].copy()
listings=listings_og.copy()

# Join updates
listings_merge_keys = [
    'from_wallet',
    'nft_collection',
    'nft_id'
]
updates = listings.loc[listings['tx_type']=='updateListing',['tx_hash','datetime','blockNumber'] + listings_merge_keys].copy()
updates.rename(columns={
    'tx_hash':'update_tx_hash',
    'datetime':'updated_at',
    'blockNumber':'update_blockNumber'
}, inplace=True)
listings_updates = listings.merge(updates, how='left', on=listings_merge_keys)
listings_updates = listings_updates.loc[listings_updates['blockNumber']<listings_updates['update_blockNumber']]
most_recent_updates = listings_updates.groupby('tx_hash',as_index=False).agg({'update_blockNumber':'min'})
listings_updates = listings_updates.merge(most_recent_updates, how='inner',on=['tx_hash','update_blockNumber']) 
dupe_cols_to_drop = list(listings_updates.columns)
dupe_cols_to_drop.remove('update_tx_hash')
listings_updates.drop_duplicates(dupe_cols_to_drop,inplace=True)
listings = listings.merge(listings_updates, how='left', on=list(listings_og.columns))

# Cancellations
cancellations = marketplace_txs_raw.loc[marketplace_txs_raw['tx_type'].isin(['cancelListing'])].copy()
cancellations = cancellations.loc[:,['tx_hash','datetime','blockNumber'] + listings_merge_keys].copy()
cancellations.rename(columns={
    'tx_hash':'cancellation_tx_hash',
    'datetime':'cancelled_at',
    'blockNumber':'cancellation_blockNumber'
}, inplace=True)
listings_cancellations = listings_og.merge(cancellations, how='left', on=listings_merge_keys)
listings_cancellations = listings_cancellations.loc[listings_cancellations['blockNumber']<=listings_cancellations['cancellation_blockNumber']] # less than or equal to to account for times when the tx is updated then immediately cancelled
most_recent_cancellation = listings_cancellations.groupby('tx_hash',as_index=False).agg({'cancellation_blockNumber':'min'})
listings_cancellations = listings_cancellations.merge(most_recent_cancellation, how='inner',on=['tx_hash','cancellation_blockNumber']) 
cancellation_merge_cols = list(listings.columns)
listings = listings.merge(listings_cancellations, how='left', on=list(listings_og.columns))
# handle cases where there is both an update and a cancellation
listings.loc[listings.cancellation_blockNumber >= listings.update_blockNumber, ['cancellation_tx_hash', 'cancelled_at']] = np.nan
listings.loc[listings.cancellation_blockNumber < listings.update_blockNumber, ['update_tx_hash', 'updated_at']] = np.nan

# Sales
sales_table_merge_keys = [
    'wallet_seller',
    'nft_collection',
    'nft_id'
]
sales = sales.loc[:,['tx_hash','datetime','blockNumber','quantity'] + sales_table_merge_keys].copy()
sales.rename(columns={
    'tx_hash':'final_sale_tx_hash',
    'datetime':'sold_at',
    'blockNumber':'sale_blockNumber',
    'quantity':'quantity_sold'
}, inplace=True)
listings_sales = listings_og.merge(sales, how='left', left_on=listings_merge_keys, right_on=sales_table_merge_keys)
listings_sales['sold_at'] = listings_sales['sold_at'].apply(lambda x: x.tz_localize(tz))
listings_sales = listings_sales.loc[listings_sales['blockNumber']<=listings_sales['sale_blockNumber']]
listings_sales.sort_values(['tx_hash','sold_at'],inplace=True)
listings_sales['cum_quantity_sold'] = listings_sales.groupby('tx_hash').quantity_sold.cumsum()
listings_sales = listings_sales.loc[listings_sales['quantity']==listings_sales['cum_quantity_sold']]
listings = listings.merge(listings_sales, how='left', on=list(listings_og.columns))
# handle cases where there is any two of an update, a cancellation, or a listing
listings.loc[listings.cancellation_blockNumber > listings.sale_blockNumber, ['cancellation_tx_hash', 'cancelled_at']] = np.nan
listings.loc[listings.cancellation_blockNumber < listings.sale_blockNumber, ['final_sale_tx_hash','sold_at','quantity_sold']] = np.nan
listings.loc[listings.update_blockNumber > listings.sale_blockNumber, ['update_tx_hash', 'updated_at']] = np.nan
listings.loc[listings.update_blockNumber <= listings.sale_blockNumber, ['final_sale_tx_hash','sold_at','quantity_sold']] = np.nan

listings.drop('wallet_seller',axis=1,inplace=True)
listings.rename(columns={
    'datetime':'listed_at',
    'expiration_datetime':'expires_at',
    'from_wallet':'wallet_seller'
}, inplace=True)
listings['listing_price_magic'] = listings['listing_price_magic'].apply(lambda x: round(x,2))

cols_to_load = [
    'tx_hash',
    'listed_at',
    'wallet_seller',
    'listing_price_magic',
    'gas_fee_eth',
    'nft_collection',
    'nft_id',
    'nft_name',
    'nft_subcategory',
    'quantity',
    'update_tx_hash',
    'cancellation_tx_hash',
    'final_sale_tx_hash',
    'updated_at',
    'cancelled_at',
    'sold_at',
    'expires_at'
]

listings = listings.loc[:,cols_to_load]

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

listings.to_sql(
    'marketplace_listings', 
    con = connection, 
    if_exists = 'append', 
    chunksize = 1000,
    index=False
)

connection.close()
engine.dispose()

## NOTES ON BUGS FIXED:
###########################
### UPDATES
# Scenario 1: there is an exact duplicate of the tx except for the hash and the nonce.
# probably a situation where the tx got stuck and overridden?
# solution: drop duplicates
# this seems to have taken care of everything (for now)
# for col in listings.columns:
#     print('{}: {}, {}'.format(col,listings.loc[listings.hash=='0x5f1a67bcd72f468cc03d0e961a0c15d3a66e0ea31b649b36d04646e9bd90905d',col].values[0],listings.loc[listings.hash=='0xab714bacfd4f33d850c7740d73d564f8625fe3f748422ace8bfb2718f7fc6416',col].values[0]))
###########################

###########################
# Scenario 1: the first cancellation attempt failed
# solution: filter out txs where txreceipt_status = 0
# should prob do this at the top of the script
# This fix cleared up all my issues
# for hash in listings_cancellations.loc[listings_cancellations.tx_hash=='0x119a7402ad31072a2dbf1ba93483f36c834a6a3eb211caf9fb932c081cedb320','cancellation_tx_hash']:
#     print(hash)
###########################

# Sales are especially tricky because we can have 'partial sales'
# for full sales we can treat it as a normal termination event like a cancellation
# but for partial sales we need to note the quantity sold and then, if it is less than the quantity listed,
# we need to create a new entry for the listing with the same tx_hash as the original listing,
# the same timstamp as the sale, but an updated quantity

# sometimes updates and other txs are duplicated for real. 
# See 0xd12546fc116f1a987a0b3a7293542dcb190de7c4791e46144edcd9084c477977 and
# 0xb0569348c43dc519f934be41ee5236c63e04b402e9a8b658fc7b137c8ffe281c

# Finally, there are some cases where expires_at is less than updated_at, sold_at, or
# cancelled_at. It seems that this is related to a bug in the marketplace preventing the 
# expiry function from working properly. If this is the case we may consider nullifying
# expiry date until that is fixed. To be updated.