
# coding: utf-8

# In[46]:


import sys
import random
import requests
import json

import decimal
import pymysql
import pickle
import os.path
import operator


from utils import first_block_of_day, last_block_of_day
from utils import exeSQL, hex2wei, wei2eth
from multiprocessing import Pool

from utils import db_config

from tqdm import tqdm as tqdm_no_notebook
from tqdm import tqdm_notebook

day_start = '20151001'
day_until = '20151231'

feature_table_name = "cluster_features_" + day_start + "_" + day_until
action_table_name = "action_" + day_start + "_" + day_until + "_nointernal_noreward"

parity_url = 'http://127.0.0.1:8545'

par_num = 40


# In[47]:


block_start = first_block_of_day(int(day_start[:4]), int(day_start[4:6]), int(day_start[6:8]))
block_until = last_block_of_day(int(day_until[:4]), int(day_until[4:6]), int(day_until[6:8]))
print (block_start)
print (block_until)

def is_ijupyter():
    return 'ipykernel' in sys.modules

tqdm = tqdm_notebook if is_ijupyter() else tqdm_no_notebook


# In[48]:


def hex2eth(s):
    return wei2eth(hex2wei(s))

def fetchAddressSet(account_type):
    fetch_addrs_sql = ("SELECT {} FROM {}").format(account_type, action_table_name)
    addrs = exeSQL(fetch_addrs_sql)
    flat_addrs = [item for sublist in addrs for item in sublist]
    return set((flat_addrs))

def fetchAddr():
    if os.path.isfile('total_addr_set_20151001_20151231.pkl'):
        with open('total_addr_set_20151001_20151231.pkl', 'rb') as f:
            total_addr_set = pickle.load(f)
    else:
        source_set = fetchAddressSet('source')
        target_set = fetchAddressSet('target')
        total_addr_set = source_set.union(target_set)
        with open('total_addr_set_20151001_20151231.pkl', 'wb') as f:
            pickle.dump(total_addr_set, f)
    return total_addr_set

def checkBalance(addr, block_num):
    data_json = {"method":"eth_getBalance",
            "params":[addr, block_num],
            "id": random.randint(1, 10000), "jsonrpc":"2.0"}
    headers_json = {"Content-Type": "application/json"}
    r = requests.post(parity_url, headers=headers_json, data=json.dumps(data_json))
    rj = r.json()
    if rj['id'] ==  data_json['id']:
        if 'result' in rj:
            return hex2eth(rj['result'])
        else:
            print ("Addr: " + addr + " , rj: " + str(rj))
            return None
    else:
        return None


# In[49]:


def dropClusterFeatureTable():
    drop_table_balance = ("DROP TABLE IF EXISTS {}".format(feature_table_name))
    exeSQL(drop_table_balance, True)

def initClusterFeaturesTable():
    create_table_balance = ("CREATE TABLE {} ("
                            "id INT NOT NULL AUTO_INCREMENT,"
                            "address CHAR(42),"
                            "split_point_start INT NOT NULL, "
                            "split_point_stop INT NOT NULL, "
                            "in_num INT NOT NULL,"
                            "in_ethers FLOAT NOT NULL,"
                            "in_addrs_set_len INT NOT NULL,"
                            "in_dec_places INT NOT NULL,"
                            "out_num INT NOT NULL,"
                            "out_ethers FLOAT NOT NULL,"
                            "out_addrs_set_len INT NOT NULL,"
                            "out_dec_places INT NOT NULL,"
                            "init_balance FLOAT NOT NULL,"
                            "final_balance FLOAT NOT NULL,"
                            "PRIMARY KEY(id)) ; ".format(feature_table_name))
    exeSQL(create_table_balance, True)

def queryClusterFeatures(addr):
    query_feature_sql = ("SELECT * FROM {} WHERE address = '{}' limit 1").format(feature_table_name, addr)
    features = exeSQL(query_feature_sql)
    return features

def insertClusterFeatures(split_features):
    connection = pymysql.connect(**db_config)
    try:
        with connection.cursor() as cursor:
            for feature in split_features:
                insert_feature_sql = """INSERT INTO {} (address, split_point_start, split_point_stop,
                        in_num, in_ethers, in_addrs_set_len, in_dec_places,
                        out_num, out_ethers, out_addrs_set_len, out_dec_places,
                        init_balance, final_balance) VALUES ('{}', {}, {}, {}, {},
                        {}, {}, {}, {}, {}, {}, {}, {})""".format(feature_table_name, feature[0], feature[1], feature[2],
                                                                feature[3], feature[4], feature[5],
                                                                feature[6], feature[7], feature[8],
                                                                feature[9], feature[10], feature[11], feature[12])
                cursor.execute(insert_feature_sql)
            connection.commit()
    except Exception as e:
        print ("Insert Cluster Features Error: ", e)
    finally:
        connection.close()


# In[50]:


def insertFeaturesPerAddr(addr):
    if (len(queryClusterFeatures(addr)) > 0):
        return
    fetch_txs_sql = ("SELECT * FROM {} "
                    "WHERE (source = '{}' OR target = '{}') ").format(action_table_name, addr, addr)
    txs = exeSQL(fetch_txs_sql)
    # Sort the txs
    sorted_txs = sorted(txs, key=operator.itemgetter(6, 7, 8))
    split_points = []
    last_tx_type = 'out'
    for index, tx in enumerate(sorted_txs):
        if tx[2] == addr:
            last_tx_type = 'out'
            continue
        else:
            if last_tx_type == 'out':
                split_points.append(index)
            last_tx_type = 'in'
    split_points.append(len(sorted_txs))
    split_features = []
#     print (split_points)
    for i in range(len(split_points)-1):
        sub_txs = sorted_txs[split_points[i]:split_points[i+1]]
        in_num = 0
        out_num = 0
        in_ethers = 0
        out_ethers = 0
        in_addrs_set = set()
        out_addrs_set = set()
        in_ethers_decimal_places_dict = {}
        out_ethers_decimal_places_dict = {}
        for tx in sub_txs:
            if tx[2] == addr:
                out_num += 1
                out_ethers += int(tx[4], 16) / 10**18
                out_addrs_set.add(tx[3])
                out_ethers_decimal_places = - decimal.Decimal(str(int(tx[4], 16) / 10**18)).as_tuple().exponent
                if out_ethers_decimal_places in out_ethers_decimal_places_dict:
                    out_ethers_decimal_places_dict[out_ethers_decimal_places] += 1
                else:
                    out_ethers_decimal_places_dict[out_ethers_decimal_places] = 1
            else:
                in_num += 1
                in_ethers += int(tx[4], 16) / 10**18
                in_addrs_set.add(tx[2])
                in_ethers_decimal_places = - decimal.Decimal(str(int(tx[4], 16) / 10**18)).as_tuple().exponent
                if in_ethers_decimal_places in in_ethers_decimal_places_dict:
                    in_ethers_decimal_places_dict[in_ethers_decimal_places] += 1
                else:
                    in_ethers_decimal_places_dict[in_ethers_decimal_places] = 1
        init_balance = checkBalance(addr, hex(sub_txs[0][6]-1))
        final_balance = checkBalance(addr, hex(sub_txs[-1][6]))
        max_freq_in_ethers_decimal_places = max(in_ethers_decimal_places_dict, key=in_ethers_decimal_places_dict.get) if len(in_ethers_decimal_places_dict)>0 else -1
        max_freq_out_ethers_decimal_places = max(out_ethers_decimal_places_dict, key=out_ethers_decimal_places_dict.get) if len(out_ethers_decimal_places_dict)>0 else -1
        fecture = [addr, split_points[i], split_points[i+1], in_num, in_ethers, len(in_addrs_set), max_freq_in_ethers_decimal_places,
              out_num, out_ethers, len(out_addrs_set), max_freq_out_ethers_decimal_places,
              init_balance, final_balance]
        split_features.append(fecture)
#     return split_features
#     print (split_features)
    insertClusterFeatures(split_features)

# print (splitTxsPerAddr('0x571fd0e7d4995c4d0900d04943eaf324682d7c9c'))


# In[51]:


def insertFeaturesFromAddrsSet(addrs_set, index):
#     actions_dict = {}
    text = "Progreeser #{}".format(index)
    for addr in tqdm(addrs_set, desc=text, position=index):
#         if addr == 'none00000000000000000000000000000000000000':
#             continue
        insertFeaturesPerAddr(addr)

# def fetchBalancesAllAddrs(block_start, block_until):
#     total_addr_set = fetchAddr(block_start, block_until)
#     return total_addr_set

def insertFeaturesAllAddrs():
    total_addr_set = fetchAddr()
    slen = len(total_addr_set) // par_num
    addr_sets_list = []
    index_list = list(range(par_num))
    for i in range(par_num):
        if i < par_num-1:
            addr_sets_list.append(set(random.sample(total_addr_set, slen)))
            total_addr_set -= addr_sets_list[i]
        else:
            addr_sets_list.append(total_addr_set)
#     print (len(addr_sets_list))
    Pool(par_num).starmap(insertFeaturesFromAddrsSet, zip(addr_sets_list, index_list))


# In[52]:


# dropClusterFeatureTable()
initClusterFeaturesTable()
# split_features = splitTxsPerAddr('0x571fd0e7d4995c4d0900d04943eaf324682d7c9c')
# insertClusterFeatures(split_features)
insertFeaturesAllAddrs()


# In[55]:


# insertFeaturesPerAddr('0x973e0cd997d426f38927407568d9857a7249ac92')

