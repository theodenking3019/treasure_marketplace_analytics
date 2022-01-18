import json
import os
import pandas as pd
from sqlalchemy import create_engine
from web3 import Web3

os.chdir("v2_mysql/treasure_prices_l1/pull_l1_treasure_data")

with open("../flask_app/mysql_credential.json") as sqlCredentialFile:
    mysqlCredential = json.loads(sqlCredentialFile.read())
treasureContractAddress = "0xf3DFbE887D81C442557f7a59e3a0aEcf5e39F6aa"
with open("infura_key.json") as infuraKeyFile:
    infuraKey = json.loads(infuraKeyFile.read())
    infuraKey = infuraKey['key']
with open("treasure_abi.json") as treasureABIFile:
    treasureABI = json.loads(treasureABIFile.read())

web3 = Web3(Web3.HTTPProvider(infuraKey))
treasureContract = web3.eth.contract(address=treasureContractAddress, abi=treasureABI)
treasureContract.functions.getAsset1(1).call()
treasureItems = {}
for i in range(1,9000):
    itemList = []
    itemList.append(treasureContract.functions.getAsset1(i).call())
    itemList.append(treasureContract.functions.getAsset2(i).call())
    itemList.append(treasureContract.functions.getAsset3(i).call())
    itemList.append(treasureContract.functions.getAsset4(i).call())
    itemList.append(treasureContract.functions.getAsset5(i).call())
    itemList.append(treasureContract.functions.getAsset6(i).call())
    itemList.append(treasureContract.functions.getAsset7(i).call())
    itemList.append(treasureContract.functions.getAsset8(i).call())
    treasureItems[i] = itemList
# with open('l1_treasure_data.json', 'w+') as f:
#     json.dump(treasureItems, f)

treasureItemsStringified = {}
for key, value in treasureItems.items():
    treasureItemsStringified[key] = str(value)

treasureDF = pd.DataFrame.from_dict(treasureItemsStringified, orient='index')
treasureDF.reset_index(inplace=True)
treasureDF.rename(columns={"index":"id", 0:"item_list"}, inplace=True)

engine = create_engine(
    "mysql+pymysql://{user}:{pw}@{host}/{db}".format(
    user=mysqlCredential['username'], 
    pw=mysqlCredential['pw'], 
    host=mysqlCredential['host'], 
    db="treasure"
    )
)
connection = engine.connect()
treasureDF.to_sql(
    'attributes_l1_treasures', 
    con = connection, 
    if_exists = 'append', 
    chunksize = 1000,
    index=False
    )
connection.close()
engine.dispose()