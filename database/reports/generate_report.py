# In this file we'll generate charts and save them in a daily HTML report

# import packages
import datetime as dt
import os
import sqlite3

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

sns.set_style('whitegrid')
sns.despine()

# read in data
connection = sqlite3.connect("treasure.db")
cursor = connection.cursor()
cursor.execute("SELECT * FROM marketplace_sales;")
marketplace_sales = cursor.fetchall()
marketplace_sales = pd.DataFrame(marketplace_sales)
marketplace_sales.columns = [description[0] for description in cursor.description]

# column manipulations as necessary
marketplace_sales['timestamp'] = pd.to_datetime(marketplace_sales['timestamp'])
marketplace_sales['timestamp'] = marketplace_sales['timestamp'].dt.tz_localize(tz='UTC')
marketplace_sales['timestamp'] = marketplace_sales['timestamp'].dt.tz_convert(tz='US/Eastern')
marketplace_sales['date'] = marketplace_sales['timestamp'].dt.date

## build charts
# total volume
daily_sales = marketplace_sales.groupby(['date'], as_index=False).agg({'quantity':'sum', 'sale_amt_magic':'sum'})

sns.barplot(daily_sales.date,daily_sales.quantity, color='blue')
plt.title("# of Sales on Treasure Marketplace per Day")
plt.xlabel('Date')
plt.ylabel('# of NFT Sales')
plt.xticks(rotation = 45)
plt.show()

g = sns.barplot(daily_sales.date,daily_sales.sale_amt_magic, color='blue')
plt.title("Sales in $MAGIC on Treasure Marketplace per Day")
plt.xlabel('Date')
plt.ylabel('# of NFT Sales')
plt.xticks(rotation = 45)
ylabels = ['{:,.0f}'.format(y) + 'K' for y in g.get_yticks()/1000]
g.set_yticklabels(ylabels)
plt.show()

# volume by collection
daily_sales_by_collection = marketplace_sales.groupby(['date', 'nft_collection'], as_index=False).agg({'quantity':'sum', 'sale_amt_magic':'sum'})

ax = sns.histplot(
    daily_sales_by_collection,
    x='date',
    weights='quantity',
    hue='nft_collection',
    multiple='stack',
    edgecolor='white',
    shrink=0.8
)
plt.title("# of Sales on Treasure Marketplace per Day")
plt.xlabel('Date')
plt.ylabel('# of NFT Sales')
plt.xticks(rotation = 45)
plt.show()

g = sns.histplot(
    daily_sales_by_collection,
    x='date',
    weights='sale_amt_magic',
    hue='nft_collection',
    multiple='stack',
    edgecolor='white',
    shrink=0.8
)
plt.title("Sales in $MAGIC on Treasure Marketplace per Day")
plt.xlabel('Date')
plt.ylabel('Sales in $MAGIC')
plt.xticks(rotation = 45)
ylabels = ['{:,.0f}'.format(y) + 'K' for y in g.get_yticks()/1000]
g.set_yticklabels(ylabels)
plt.show()

# average sale price by collection
med_sales_by_collection = marketplace_sales.groupby(['date', 'nft_collection'], as_index=False).agg({'sale_amt_magic':'median'})

g = sns.lineplot(
    data=med_sales_by_collection.loc[med_sales_by_collection.nft_collection!='extra_life'],
    x='date',
    y='sale_amt_magic',
    hue='nft_collection',
)
plt.title("Median Sale Price by NFT Collection")
plt.xlabel('Date')
plt.ylabel('Median Sale Price in $MAGIC')
plt.ylim(0, 4000)
plt.xticks(rotation = 45)
ylabels = ['{:,.0f}'.format(y) + 'K' for y in g.get_yticks()/1000]
plt.show()

# average sale price by legion and treasures
med_sales_by_nft_legions = marketplace_sales.loc[marketplace_sales['nft_collection']=='legions_genesis'].groupby(['date', 'nft_subcategory'], as_index=False).agg({'sale_amt_magic':'median'})

g = sns.lineplot(
    data=med_sales_by_nft_legions,
    x='date',
    y='sale_amt_magic',
    hue='nft_subcategory',
)
plt.title("Median Sale Price by NFT")
plt.xlabel('Date')
plt.ylabel('Median Sale Price in $MAGIC')
# plt.ylim(0, 4000)
# ylabels = ['{:,.0f}'.format(y) + 'K' for y in g.get_yticks()/1000]
plt.xticks(rotation = 45)
plt.show()


treasures_df = marketplace_sales.loc[marketplace_sales['nft_collection']=='treasures']
treasures_first10 = treasures_df.loc[treasures_df.nft_name.isin(treasures_df.nft_name.unique()[40:51])]

med_sales_by_nft_treasures = treasures_first10.groupby(['date', 'nft_subcategory'], as_index=False).agg({'sale_amt_magic':'median'})

g = sns.lineplot(
    data=med_sales_by_nft_treasures,
    x='date',
    y='sale_amt_magic',
    hue='nft_subcategory',
)
plt.title("Median Sale Price by NFT")
plt.xlabel('Date')
plt.ylabel('Median Sale Price in $MAGIC')
# plt.ylim(0, 4000)
# ylabels = ['{:,.0f}'.format(y) + 'K' for y in g.get_yticks()/1000]
plt.xticks(rotation = 45)
plt.show()
