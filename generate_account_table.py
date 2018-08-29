
# coding: utf-8

# In[1]:


import os, pickle, random, requests, json, sys
from utils import exeSQL, insert_account

from tqdm import tqdm as tqdm_no_notebook
from tqdm import tqdm_notebook

parity_url = 'http://127.0.0.1:8545'
table_name = 'account'

def is_ijupyter():
    return 'ipykernel' in sys.modules

tqdm = tqdm_notebook if is_ijupyter() else tqdm_no_notebook


# In[2]:


recreate = False
if recreate:
#    drop_table('action_20151001_20151231')
    create_account_table()


# In[3]:


def fetch_total_addr():
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


# In[4]:


def create_account_table():
    create_account_sql = """CREATE TABLE account ( `id` int(11) NOT NULL AUTO_INCREMENT,
                `address` char(42) NOT NULL, `kind` enum('normal','sc','none') NOT NULL,
                PRIMARY KEY (`id`)) ENGINE=InnoDB"""
    exeSQL(create_account_sql, True)
    
def drop_table():
    drop_table_sql = """DROP TABLE account"""
    exeSQL(drop_table_sql, True)


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


def insert_amount_type():
    total_addr_set = fetch_total_addr()
    for addr in tqdm(total_addr_set):
        if len(query_account(addr)) > 0:
            continue
        account_type = eth_getCode(addr)
        insert_account(addr, account_type, table_name)


# In[8]:


insert_amount_type()

