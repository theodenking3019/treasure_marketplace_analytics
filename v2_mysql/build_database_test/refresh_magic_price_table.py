# In this file we will use the coinmarketcap API to pull the price of MAGIC
# and ETH in USD for every 15 minutes historically.
# We can then join the table to the marketplace sales and floor price tables
# to get prices in either USD or ETH.

import datetime as dt
import json
import os
import pandas as pd
import pymysql
import pytz
import requests
from sqlalchemy import create_engine

os.chdir('v2_mysql/build_database_test')
tz = pytz.timezone('UTC')

# read in credentials
sql_credential = os.path.join("constants", "mysql_credential.json")
with open(sql_credential) as f:
    mysql_credentials = json.loads(f.read())

# connect to database
engine = create_engine(
    "mysql+pymysql://{user}:{pw}@{host}/{db}".format(
    user=mysql_credentials['username'], 
    pw=mysql_credentials['pw'], 
    host=mysql_credentials['host'], 
    db="treasure_test"
    )
)
connection = engine.connect()

# define function to go from json output to clean dataframe
def wrangle_price_data(response_json, token_name):
    response_json = pd.DataFrame(response_json['prices'])
    response_json.rename(columns={0:'timestamp',1:'price_{}_usd'.format(token_name)},inplace=True)
    response_json['datetime'] = [dt.datetime.fromtimestamp(int(x)/1000, tz) for x in response_json['timestamp']]
    response_json['datetime'] = response_json.datetime.dt.floor('5min') # truncate to first 5 min
    response_json.drop('timestamp',axis=1,inplace=True)
    response_json.drop_duplicates('datetime', inplace=True)  

    return response_json

# pull token prices from CoinGecko API
magic_request_url = 'https://api.coingecko.com/api/v3/coins/magic/market_chart?vs_currency=usd&days=1'
eth_request_url = 'https://api.coingecko.com/api/v3/coins/ethereum/market_chart?vs_currency=usd&days=1'

magic_response = requests.get(magic_request_url)
eth_response = requests.get(eth_request_url)

magic_prices = magic_response.json()
eth_prices = eth_response.json()

# wrangle data
magic_prices = wrangle_price_data(magic_prices, 'magic')
eth_prices = wrangle_price_data(eth_prices, 'eth')
merged_prices = magic_prices.merge(eth_prices, how='inner',on='datetime')
merged_prices = merged_prices.loc[:,['datetime','price_magic_usd','price_eth_usd']]

# read in latest timestamp from token_prices table
max_existing_dt_query = connection.execute('SELECT MAX(datetime) FROM token_prices')
for row in max_existing_dt_query:
    max_existing_dt = row[0]
pd.to_datetime(max_existing_dt).tz_localize(tz)
merged_prices = merged_prices.loc[merged_prices['datetime']>pd.to_datetime(max_existing_dt).tz_localize(tz)]

# insert records
merged_prices.to_sql(
    'token_prices', 
    con = connection, 
    if_exists = 'append', 
    chunksize = 1000,
    index=False
)

connection.close()