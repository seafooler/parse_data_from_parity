
# coding: utf-8

# In[1]:


import os, pickle, random, requests, json, sys, pickle, os
from utils import exeSQL, insert_account, fetchAddressSet, insert_multiple_accounts
from multiprocessing import Pool

from tqdm import tqdm as tqdm_no_notebook
from tqdm import tqdm_notebook

parity_url = 'http://127.0.0.1:8545'
table_name = 'account_2017_1'

def is_ijupyter():
    return 'ipykernel' in sys.modules

tqdm = tqdm_notebook if is_ijupyter() else tqdm_no_notebook
par = 20

# In[2]:

def create_account_table():
    create_account_sql = """CREATE TABLE account_2017_1 ( `id` int(11) NOT NULL AUTO_INCREMENT,
                `address` char(42) NOT NULL, `kind` enum('normal','sc','none') NOT NULL,
                PRIMARY KEY (`id`)) ENGINE=InnoDB"""
    exeSQL(create_account_sql, True)
    
def drop_table():
    drop_table_sql = """DROP TABLE account_2017_1"""
    exeSQL(drop_table_sql, True)

# recreate = True
# if recreate:
#     drop_table()
#     create_account_table()


# In[3]:


def fetch_total_addr():
    if os.path.isfile('total_addr_set_20170101_20170331.pkl'):
        with open('total_addr_set_20170101_20170331.pkl', 'rb') as f:
            total_addr_set = pickle.load(f)
    else:
        source_set = fetchAddressSet('source', 'txs_20170101_20170331')
        target_set = fetchAddressSet('target', 'txs_20170101_20170331')
        total_addr_set = source_set.union(target_set)
        with open('total_addr_set_20170101_20170331.pkl', 'wb') as f:
            pickle.dump(total_addr_set, f)
    return total_addr_set


# In[5]:


def eth_getCode(addr):
    data_json = {"method":"eth_getCode",
            "params":[addr, "latest"],
            "id": random.randint(1, 10000), "jsonrpc":"2.0"}
    headers_json = {"Content-Type": "application/json"}
    r = requests.post(parity_url, headers=headers_json, data=json.dumps(data_json))
    rj = r.json()
    if rj['id'] ==  data_json['id']:
        if 'result' in rj:
            if rj['result'] == '0x':
                return 'normal'
            else:
                return 'sc'
        else:
            print ("Addr: " + addr + " , rj: " + str(rj))
            return None
    else:
        return None


# In[6]:


def query_account(addr):
    query_account_sql = ("SELECT * FROM {} WHERE address = '{}' limit 1").format(table_name, addr)
    query_account_sql_result = exeSQL(query_account_sql)
    return query_account_sql_result


# In[7]:

def split_accounts(total_addr_set, par):
    split_account_list_prefix = "split_accouts_2017_1_part_"
    if not (os.path.isfile(split_account_list_prefix+str(1))):
        total_addr_list = list(total_addr_set)
        # Split
        sublist_len = len(total_addr_list) // par
        sublists = []
        for i in range(par-1):
            tmp_list = total_addr_list[i*sublist_len:(i+1)*sublist_len]
            sublists.append(tmp_list)
        tmp_list = total_addr_list[(par-1)*sublist_len:]
        sublists.append(tmp_list)
        # Dump
        for i in range(par):
            with open(split_account_list_prefix+str(i), 'wb') as f:
                pickle.dump(sublists[i], f)
    return split_account_list_prefix


def insert_account_type(index):
    split_account_list_prefix = "split_accouts_2017_1_part_"
    with open(split_account_list_prefix+str(index), 'rb') as f:
        addr_set = pickle.load(f)
    count = 0
    parsed_entries = []
    for addr in tqdm(addr_set, position=index, desc="Progressor {}:".format(index)):
        if len(query_account(addr)) > 0:
            continue
        account_type = eth_getCode(addr)
        count += 1
        parsed_entries.append([addr, account_type])
        if count == 20:
            insert_multiple_accounts(parsed_entries, table_name)
            count = 0
            parsed_entries = []
    if count > 0:
        insert_multiple_accounts(parsed_entries, table_name)


def main():
    total_addr_set = fetch_total_addr()
    print (len(total_addr_set))
    split_account_list_prefix = split_accounts(total_addr_set, par)
    split_account_lists_indexs = []
    for i in range(par):
        split_account_lists_indexs.append(i)
    p = Pool(par)
    results = p.map(insert_account_type, split_account_lists_indexs)
    p.close()
    


# In[8]:


main()

