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
DAO_ROYALTY_PCT = 0.05
os.chdir('v2_mysql/build_database_test')

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
    marketplace_txs_old_contract = get_contract_transactions(arbiscan_api_key, contract_addresses['treasure_marketplace'], start_block=start_block)
    marketplace_txs_new_contract = get_contract_transactions(arbiscan_api_key, contract_addresses['treasure_marketplace_2'], start_block=start_block)
    marketplace_txs_old_df = pd.DataFrame.from_dict(marketplace_txs_old_contract["result"])
    marketplace_txs_new_df = pd.DataFrame.from_dict(marketplace_txs_new_contract["result"])
    marketplace_txs_old_df['contract'] = contract_addresses['treasure_marketplace']
    marketplace_txs_new_df['contract'] = contract_addresses['treasure_marketplace_2']
    marketplace_txs_df = pd.concat([marketplace_txs_old_df, marketplace_txs_new_df])

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
    marketplace_txs_raw['to'] = ['0x' + x[162:212] for x in marketplace_txs_raw["input"]]
    marketplace_txs_raw["tx_type"] = marketplace_txs_raw["tx_type"].map(method_ids)
    marketplace_txs_raw = marketplace_txs_raw.loc[~pd.isnull(marketplace_txs_raw["tx_type"])] # null transactions are all whitelisting of certain accounts before marketplace launch
    marketplace_txs_raw['datetime'] = [dt.datetime.fromtimestamp(int(x), tz) for x in marketplace_txs_raw['timeStamp']]
    marketplace_txs_raw['gas_fee_eth'] = (marketplace_txs_raw['gasPrice'].astype('int64') * 1e-9 * marketplace_txs_raw['gasUsed'].astype(int) * 1e-9) / 2.0
    marketplace_txs_raw['nft_collection'] = [contract_addresses[x[33:74]] if x[33:74] in contract_addresses.keys() else x[33:74] for x in marketplace_txs_raw['input']] # works for both types of txs
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
        'tx_type',
        'contract'
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
    mkt_magic_merged_table.loc[mkt_magic_merged_table['dao_amt_received_magic']==mkt_magic_merged_table['seller_amt_received_magic'], 'sale_amt_magic'] = \
        mkt_magic_merged_table.loc[mkt_magic_merged_table['dao_amt_received_magic']==mkt_magic_merged_table['seller_amt_received_magic'], 'seller_amt_received_magic'] / 0.95
    mkt_magic_merged_table.loc[mkt_magic_merged_table['dao_amt_received_magic']==mkt_magic_merged_table['seller_amt_received_magic'], 'dao_amt_received_magic'] = \
        mkt_magic_merged_table.loc[mkt_magic_merged_table['dao_amt_received_magic']==mkt_magic_merged_table['seller_amt_received_magic'], 'sale_amt_magic'] * 0.05

    marketplace_sales = marketplace_sales.merge(mkt_magic_merged_table, how='inner', on='hash')
    marketplace_sales.rename(columns={
        'hash':'tx_hash',
        'to':'wallet_seller',
        'from':'wallet_buyer'
        },inplace=True)

    marketplace_sales.loc[marketplace_sales['contract']==contract_addresses['treasure_marketplace_2'], 'sale_amt_magic'] = marketplace_sales.loc[marketplace_sales['contract']==contract_addresses['treasure_marketplace_2'], 'seller_amt_received_magic']
    marketplace_sales.loc[marketplace_sales['contract']==contract_addresses['treasure_marketplace_2'], 'seller_amt_received_magic'] = marketplace_sales.loc[marketplace_sales['contract']==contract_addresses['treasure_marketplace_2'], 'sale_amt_magic'] * 0.95
    marketplace_sales.loc[marketplace_sales['contract']==contract_addresses['treasure_marketplace_2'], 'dao_amt_received_magic'] = marketplace_sales.loc[marketplace_sales['contract']==contract_addresses['treasure_marketplace_2'], 'sale_amt_magic'] * 0.05

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
        db="treasure_test"
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

    # write data to sql
    marketplace_sales_df.to_sql(
        'marketplace_sales', 
        con = connection, 
        if_exists = 'append', 
        chunksize = 1000,
        index=False
        )
    marketplace_listings_df.to_sql(
        'marketplace_listings', 
        con = connection, 
        if_exists = 'append', 
        chunksize = 1000,
        index=False
        )

    connection.close()
    engine.dispose()

refresh_database(mysql_credentials)


