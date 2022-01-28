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
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from ratelimit import limits, sleep_and_retry

ONE_SECOND = 1
DAO_WALLET = '0xdb6ab450178babcf0e467c1f3b436050d907e233'
os.chdir('build_database_test')
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

# define functions needed for final script
@sleep_and_retry
@limits(calls=2, period=ONE_SECOND)
def get_contract_transactions(arbiscan_api_key, from_address=None, contract_address=None, start_block=0, tx_type="txlist"):

    request_url = "https://api.arbiscan.io/api"
    request_url = request_url + "?module=account"
    request_url = request_url + "&action=" + tx_type
    if from_address:
        request_url = request_url + "&address=" + from_address
    if contract_address:
        request_url = request_url + "&contractaddress=" + contract_address
    request_url = request_url + "&startblock=" + str(start_block)
    request_url = request_url + "&endblock=99999999"
    request_url = request_url + "&sort=desc"
    request_url = request_url + "&apikey=" + arbiscan_api_key

    response = requests.get(request_url)
    return response.json()

magic_txs = get_contract_transactions(arbiscan_api_key, from_address=None, contract_address=contract_addresses['magic'], tx_type="tokentx")
magic_txs_df = pd.DataFrame.from_dict(magic_txs["result"])

magic_txs_df.head()

magic_txs_df.columns
magic_txs_df.input

# txs to parse
# swap ETH for magic (ex https://arbiscan.io/tx/0x6294e7eedf3429c9ae328c14eb6e564239f0006ed018276824b23f1cb43d1667)
# swap tokens for tokens (ex https://arbiscan.io/tx/0x13a59f69bdb4404e94937734a09a2eb0517cd64dfdc2713c97c9fd987e88e1c5,
# https://arbiscan.io/tx/0x0802838fe255433c3a8e0a6130da2b72d3944460c5428d9cf15a2ff1e220b8e2,
# https://arbiscan.io/tx/0x464acd162c7dce7cd19cc8e52b4ff3f5f115d5dc1645dec8675febb5f09784f2,
# https://arbiscan.io/tx/0x63f9cd4b14bcda741866688ee8cb826a5459a523f62e58b83e1cb34bc10107c3)

# add ETH liquidity (https://arbiscan.io/tx/0xaf007ec11bd6ba56b7a26e29365d80b9b64d74d19b37f55805f6400ae4f72d1f)
# remove liquidity (https://arbiscan.io/tx/0xde2492d18f22a0f1731011f1a0bd8156efbf2654c79535d06285ea6a2235b031)
# withdraw and harvest liquidity rewards (https://arbiscan.io/tx/0xeb12f3994f9ec9ce617eb22b8d0de5f9d79590ac223a1034b5e860c5b4466387)

# marketplace buys, sells, listings, cancellations (already accounted for)

# withdraw from genesis mine (https://arbiscan.io/tx/0x0a15c91156b128a85cd6fb161b86498b4a6d7f12201f3e392e34bddc0f5a33d9)
# harvest mine rewards (https://arbiscan.io/tx/0x574875ddb8236c9eae0c94d1ceb4aa42087bc590394c872d7189aec50de6a50b)
# harvest treasure rewards (https://arbiscan.io/tx/0xedd1cdc02c7931ba7334a943c8222e4656afa7e0458044a46cd99d3a824e29a3)
# stake to genesis mine (?)

# transfer (https://arbiscan.io/tx/0x56376bc5d4937e5daca0203d5a77a99ef8cc77e6a2d5355ab4a803037fa9d76e,
# https://arbiscan.io/tx/0x41a875add1453f6f4f648b947e291ad95185a7a4bbb00465515c511594588ab8)

# gOHM bond (https://arbiscan.io/tx/0x824441d68c286769bf64d6846d1209e5507df9e87a87699c8c753c4ef03aeca7)

# ??? (https://arbiscan.io/tx/0x5cdfe3cbdc43907ec18fb4d66cfc6fddbb8060e0d3102f1024e26505a962f29a,
# https://arbiscan.io/tx/0x7369d4e47b2c5bc61bb3dbb3f4eea007bbdf8fc006190007124e7e196a8ad740)

# transition to web3.toAscii(transactionID.input) to decode input data
# Pull input using the following request: https://api.arbiscan.io/api?module=proxy&action=eth_getTransactionByHash&txhash=0x364d40d6307057c37d00dc1da1ab83e3ed5822d72e297de9d566bd27172c498f&apikey=6NH87UDEBVDHEGFN7JUJ2YQNZ6J1YVMQVQ-
# have to do this for every tx unfortunately so it will take a few days to populate the db

# pilgrimage: https://arbiscan.io/tx/0xf8f920f558e563a3e37860ce8555ab3947dd6ab683bd2e7a3fe6aaf5884fa9f9

# current magic balance: https://api.arbiscan.io/api?module=account&action=tokenbalance&contractaddress=0x539bdE0d7Dbd336b79148AA742883198BBF60342&address=<<ACCOUNT>>&tag=latest&apikey=<<API KEY>>





def pull_arbiscan_data(arbiscan_api_key, method_ids, start_block=0, latest_tx_hashes=[]):
    # read in marketplace txs
    marketplace_txs = get_contract_transactions(arbiscan_api_key, contract_addresses['treasure_marketplace'], start_block=start_block)
    marketplace_txs_df = pd.DataFrame.from_dict(marketplace_txs["result"])
    print(marketplace_txs_df.hash[0])
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

def process_marketplace_txs(marketplace_txs_raw, method_ids, contract_addresses, treasure_ids_numeric):
    marketplace_txs_raw["tx_type"] = [x[:10] for x in marketplace_txs_raw["input"]]
    marketplace_txs_raw["tx_type"] = marketplace_txs_raw["tx_type"].map(method_ids)
    marketplace_txs_raw = marketplace_txs_raw.loc[~pd.isnull(marketplace_txs_raw["tx_type"])] # null transactions are all whitelisting of certain accounts before marketplace launch
    marketplace_txs_raw['datetime'] = [dt.datetime.fromtimestamp(int(x), tz) for x in marketplace_txs_raw['timeStamp']]
    marketplace_txs_raw['gas_fee_eth'] = (marketplace_txs_raw['gasPrice'].astype('int64') * 1e-9 * marketplace_txs_raw['gasUsed'].astype(int) * 1e-9) / 2.0
    marketplace_txs_raw['nft_collection'] = [contract_addresses[x[33:74]] for x in marketplace_txs_raw['input']] # works for both types of txs
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
    columns_to_keep = [
        'hash',
        'datetime',
        'blockNumber',
        'from',
        'to',
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

    return marketplace_txs_raw.loc[:,columns_to_keep].copy()

def build_marketplace_sales_table(marketplace_txs, magic_txs):
    marketplace_sales = marketplace_txs.loc[marketplace_txs["tx_type"]=="buyItem"].copy()

    # join magic txs table to get transaction values
    magic_txs['value'] = magic_txs['value'].astype("float64") * 1e-18
    mkt_magic_merged_table = magic_txs.merge(marketplace_sales, how='inner', on='hash')
    mkt_magic_merged_table = mkt_magic_merged_table.groupby('hash',as_index=False).agg({'value':["min", "max", "sum"]})
    mkt_magic_merged_table.columns = mkt_magic_merged_table.columns.droplevel()
    mkt_magic_merged_table.rename(columns={'':'hash','min':'dao_amt_received_magic', 'max':'seller_amt_received_magic', 'sum':'sale_amt_magic'}, inplace=True)
    # for sales where we only have one of the txs, assume it is the seller amount received
    # TODO: make this more intelligent by using the actual dao wallet address
    mkt_magic_merged_table.loc[mkt_magic_merged_table['dao_amt_received_magic']==mkt_magic_merged_table['seller_amt_received_magic'], 'sale_amt_magic'] = \
        mkt_magic_merged_table.loc[mkt_magic_merged_table['dao_amt_received_magic']==mkt_magic_merged_table['seller_amt_received_magic'], 'seller_amt_received_magic'] / 0.95
    mkt_magic_merged_table.loc[mkt_magic_merged_table['dao_amt_received_magic']==mkt_magic_merged_table['seller_amt_received_magic'], 'dao_amt_received_magic'] = \
        mkt_magic_merged_table.loc[mkt_magic_merged_table['dao_amt_received_magic']==mkt_magic_merged_table['seller_amt_received_magic'], 'sale_amt_magic'] * 0.05

    marketplace_sales = marketplace_sales.merge(mkt_magic_merged_table, how='inner', on='hash')
    marketplace_sales.drop('to', axis=1, inplace=True)
    to_wallets = magic_txs.loc[magic_txs['to']!=DAO_WALLET,['hash','to']].drop_duplicates('hash')
    marketplace_sales = marketplace_sales.merge(to_wallets, how='inner', on='hash')
    marketplace_sales.rename(columns={
        'hash':'tx_hash',
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
    return marketplace_sales.loc[:, columns_to_load]

def build_marketplace_listings_table(marketplace_txs, marketplace_sales):
    # create listings table
    listings_og = marketplace_txs.loc[marketplace_txs['tx_type'].isin(['createListing','updateListing'])].copy()
    listings=listings_og.copy()

    # join updates
    listings_merge_keys = [
        'from',
        'nft_collection',
        'nft_id'
    ]
    updates = listings.loc[listings['tx_type']=='updateListing',['hash','datetime','blockNumber'] + listings_merge_keys].copy()
    updates.rename(columns={
        'hash':'update_tx_hash',
        'datetime':'updated_at',
        'blockNumber':'update_blockNumber'
    }, inplace=True)
    listings_updates = listings.merge(updates, how='left', on=listings_merge_keys)
    listings_updates = listings_updates.loc[listings_updates['blockNumber']<listings_updates['update_blockNumber']]
    most_recent_updates = listings_updates.groupby('hash',as_index=False).agg({'update_blockNumber':'min'})
    listings_updates = listings_updates.merge(most_recent_updates, how='inner',on=['hash','update_blockNumber']) 
    dupe_cols_to_drop = list(listings_updates.columns)
    dupe_cols_to_drop.remove('update_tx_hash')
    listings_updates.drop_duplicates(dupe_cols_to_drop,inplace=True)
    listings = listings.merge(listings_updates, how='left', on=list(listings_og.columns))

    # Cancellations
    cancellations = marketplace_txs.loc[marketplace_txs['tx_type'].isin(['cancelListing'])].copy()
    cancellations = cancellations.loc[:,['hash','datetime','blockNumber'] + listings_merge_keys].copy()
    cancellations.rename(columns={
        'hash':'cancellation_tx_hash',
        'datetime':'cancelled_at',
        'blockNumber':'cancellation_blockNumber'
    }, inplace=True)
    listings_cancellations = listings_og.merge(cancellations, how='left', on=listings_merge_keys)
    listings_cancellations = listings_cancellations.loc[listings_cancellations['blockNumber']<=listings_cancellations['cancellation_blockNumber']] # less than or equal to to account for times when the tx is updated then immediately cancelled
    most_recent_cancellation = listings_cancellations.groupby('hash',as_index=False).agg({'cancellation_blockNumber':'min'})
    listings_cancellations = listings_cancellations.merge(most_recent_cancellation, how='inner',on=['hash','cancellation_blockNumber']) 
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
    sales = marketplace_sales.merge(marketplace_txs.loc[:,['hash','blockNumber']], how='inner', left_on='tx_hash', right_on='hash')
    sales = sales.loc[:,['tx_hash','datetime','blockNumber','quantity'] + sales_table_merge_keys].copy()
    sales.rename(columns={
        'tx_hash':'final_sale_tx_hash',
        'datetime':'sold_at',
        'blockNumber':'sale_blockNumber',
        'quantity':'quantity_sold'
    }, inplace=True)
    listings_sales = listings_og.merge(sales, how='left', left_on=listings_merge_keys, right_on=sales_table_merge_keys)
    listings_sales = listings_sales.loc[listings_sales['blockNumber']<=listings_sales['sale_blockNumber']]
    listings_sales.sort_values(['hash','sold_at'],inplace=True)
    listings_sales['cum_quantity_sold'] = listings_sales.groupby('hash').quantity_sold.cumsum()
    listings_sales = listings_sales.loc[listings_sales['quantity']==listings_sales['cum_quantity_sold']]
    listings = listings.merge(listings_sales, how='left', on=list(listings_og.columns))
    # handle cases where there is any two of an update, a cancellation, or a listing
    listings.loc[listings.cancellation_blockNumber > listings.sale_blockNumber, ['cancellation_tx_hash', 'cancelled_at']] = np.nan
    listings.loc[listings.cancellation_blockNumber < listings.sale_blockNumber, ['final_sale_tx_hash','sold_at','quantity_sold']] = np.nan
    listings.loc[listings.update_blockNumber > listings.sale_blockNumber, ['update_tx_hash', 'updated_at']] = np.nan
    listings.loc[listings.update_blockNumber <= listings.sale_blockNumber, ['final_sale_tx_hash','sold_at','quantity_sold']] = np.nan

    listings.drop('wallet_seller',axis=1,inplace=True)
    listings.rename(columns={
        'hash':'tx_hash',
        'datetime':'listed_at',
        'expiration_datetime':'expires_at',
        'from':'wallet_seller'
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

    return listings.loc[:,cols_to_load].copy()

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
    marketplace_df_processed = process_marketplace_txs(marketplace_df, method_ids, contract_addresses_reverse_lower, treasure_ids_numeric)
    marketplace_sales_df = build_marketplace_sales_table(marketplace_df_processed, magic_df)
    marketplace_listings_df = build_marketplace_listings_table(marketplace_df_processed, marketplace_sales_df)

    # # write data to s3
    # date = dt.datetime.now()
    # marketplace_txs_filename = f'marketplace-txs/marketplace_txs_raw_{date.year}_{date.month}_{date.day}_{date.hour}_{date.minute}.csv'
    # magic_txs_filename = f'magic-txs/magic_txs_raw_{date.year}_{date.month}_{date.day}_{date.hour}_{date.minute}.csv'
    # marketplace_df.to_csv('/tmp/tmp_marketplace_txs_df.csv')
    # magic_df.to_csv('/tmp/tmp_magic_txs_df.csv')
    # s3_resource.Object('treasure-marketplace-db', marketplace_txs_filename).upload_file('/tmp/tmp_marketplace_txs_df.csv')
    # s3_resource.Object('treasure-marketplace-db', magic_txs_filename).upload_file('/tmp/tmp_magic_txs_df.csv')
    # os.remove('/tmp/tmp_marketplace_txs_df.csv')
    # os.remove('/tmp/tmp_magic_txs_df.csv')

    # # write data to sql
    # marketplace_sales_df.to_sql(
    #     'marketplace_sales', 
    #     con = connection, 
    #     if_exists = 'append', 
    #     chunksize = 1000,
    #     index=False
    #     )
    # marketplace_listings_df.to_sql(
    #     'marketplace_listings', 
    #     con = connection, 
    #     if_exists = 'append', 
    #     chunksize = 1000,
    #     index=False
    #     )

    connection.close()
    engine.dispose()

refresh_database(mysql_credentials)

