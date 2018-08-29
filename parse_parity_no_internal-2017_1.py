
# coding: utf-8

# In[125]:


from utils import first_block_of_day, last_block_of_day, rpc_to_parity
from utils import exeSQL, drop_table, exeMultipleSQL
from utils import map_args_group
from tqdm import tqdm_notebook as tqdm

date_start = '2017-01-01'
date_end = '2017-03-31'
block_start = first_block_of_day(int(date_start[:4]), int(date_start[5:7]), int(date_start[8:10]))
block_end = last_block_of_day(int(date_end[:4]), int(date_end[5:7]), int(date_end[8:10]))


# In[106]:


table_name = "txs_20170101_20170331"
par = 20


# In[107]:


def create_txs_table(table_name):
    create_txs_sql = """CREATE TABLE {} ( `id` int(11) NOT NULL AUTO_INCREMENT,
                `source` char(42) NOT NULL, `target` char(42) NOT NULL, `amount` varchar(32) NOT NULL, 
                `tx` char(66) NOT NULL, `block_num` int(11) NOT NULL, `tx_seq` int(11) NOT NULL, 
                PRIMARY KEY (`id`), KEY `block_num_index` (`block_num`), 
                FULLTEXT `target_index` (`target`), FULLTEXT `source_index` (`source`), 
                FULLTEXT `tx_index` (`tx`) ) ENGINE=InnoDB""".format(table_name)
    exeSQL(create_txs_sql, True)


# In[140]:


recreate = True
if recreate:
    drop_table(table_name)
    create_txs_table(table_name)


# In[109]:


def fetch_fields_from_txs(txs):
    entries = []
    for tx in txs:
        tmp_en = [tx['from'], tx['to'], tx['value'], tx['hash'], 
                  str(int(tx['blockNumber'], 16)), str(int(tx['transactionIndex'], 16))]
        entries.append(tmp_en)
    return entries


# In[141]:


def insert_entries(entries, table_name):
    insert_entries_sqls = []
    for en in entries:
        sql = """INSERT INTO {} (source, target, amount, tx, block_num, tx_seq) 
            VALUES ('{}', '{}', '{}', '{}', '{}', '{}')""".format(table_name, *en)
        insert_entries_sqls.append(sql)
    exeMultipleSQL(insert_entries_sqls, True)


# In[135]:


def fetch_reward_txs(block_num):
    def parse_action_reward(dict_obj, index):
        dict_action = dict_obj['action']
        if dict_action['rewardType'] == 'block': # reward-block txs are tagged by -1
            return dict_action['author'], dict_action['value'], -1
        elif dict_action['rewardType'] == 'uncle': # reward-uncle txs are tagged by -2, -3
            return dict_action['author'], dict_action['value'], index
        else:
            print ("Error................. neither block-reward nor uncle-reward {}".format(str(block_num)))
            return None
        
    method = 'trace_block'
    params = [hex(block_num)]
    actions = rpc_to_parity(method, params).json()['result']
    reward_entries = []
    uncle_index = -2
    for act in actions:
        if act['type'] == 'reward':
            t, v, i = parse_action_reward(act, uncle_index)
            tmp_index = ['0x0000000000000000000000000000000000000000', t, v, 'None', block_num, i]
            if i == -1:
                reward_entries.append(tmp_index)
            else:
                reward_entries.insert(0, tmp_index)
                uncle_index -= 1
    return reward_entries
            


# In[136]:


def parse_blocks(block_start, block_end, table_name):
    for bn in tqdm(range(block_start, block_end)):
        method = 'eth_getBlockByNumber'
        params = [hex(bn), True]
        result = rpc_to_parity(method, params).json()['result']
        txs = result['transactions']
        entries = fetch_fields_from_txs(txs)
        reward_entries = fetch_reward_txs(bn)
        total_entries = reward_entries
        total_entries.extend(entries)
#         entries.insert(0, ['0x0000000000000000000000000000000000000000', result['miner'], str(3), 'None',
#                         str(int(result['number'], 16)), str(-1)])
        insert_entries(total_entries, table_name = table_name)


# In[142]:


interval_start = block_start
flag = True

while(flag):
    if interval_start + 600 >= block_end + 1:
        interval_end = block_end + 1
        flag = False
    else:
        interval_end = interval_start + 600
    print ("Blocks from {} to {} are in process!".format(interval_start, interval_end))
    args_groups_list = []
    sub_range_len = (interval_end - interval_start) // par
    for i in range(par-1):
        tmp_group = [i*sub_range_len+interval_start, (i+1)*sub_range_len+interval_start, table_name]
        args_groups_list.append(tmp_group)
    tmp_group = [(par-1)*sub_range_len+interval_start, interval_end, table_name]
    args_groups_list.append(tmp_group)
    print (map_args_group(args_groups_list, parse_blocks))
    interval_start += 600

