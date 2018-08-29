
# coding: utf-8

# In[1]:


from utils import first_block_of_day, last_block_of_day, rpc_to_parity
from utils import drop_table, create_action_table
from utils import exeSQL, exeMultipleSQL, insert_action, insert_multiple_actions
from utils import map_list, map_args_group
from tqdm import tqdm_notebook as tqdm

date_start = '2015-10-01'
date_end = '2015-12-31'
block_start = first_block_of_day(int(date_start[:4]), int(date_start[5:7]), int(date_start[8:10]))
block_end = last_block_of_day(int(date_end[:4]), int(date_end[5:7]), int(date_end[8:10]))

table_name = "action_20151001_20151231"

par = 20


# In[2]:


print (block_start)
print (block_end)


# In[3]:


recreate = True
if recreate:
#    drop_table('action_20151001_20151231')
    create_action_table('action_20151001_20151231')


# In[4]:


def parse_action_create(dict_obj):
    if 'error' in dict_obj:
        return None
    directive = dict_obj['type']
    tx = dict_obj['transactionHash']
    block_num = dict_obj['blockNumber']
    tx_seq = dict_obj['transactionPosition']
    act_seq = 0
    dict_action = dict_obj['action']
    dict_result = dict_obj['result']
    source = dict_action['from']
    target = dict_result['address']
    amount = dict_action['value']
    parsed_entry = [directive, source, target, amount, tx, block_num, tx_seq, act_seq]
    return parsed_entry


# In[5]:


def parse_action_call(dict_obj):
    if 'error' in dict_obj:
        return None
    directive = dict_obj['type']
    tx = dict_obj['transactionHash']
    block_num = dict_obj['blockNumber']
    tx_seq = dict_obj['transactionPosition']
    act_seq = 0
    dict_action = dict_obj['action']
    source = dict_action['from']
    target = dict_action['to']
    amount = dict_action['value']
    parsed_entry = [directive, source, target, amount, tx, block_num, tx_seq, act_seq]
    return parsed_entry


# In[6]:


def parse_action_reward(dict_obj):
    dict_action = dict_obj['action']
    directive = dict_obj['type'] + "-" + dict_action['rewardType']
    tx = dict_obj['transactionHash']
    block_num = dict_obj['blockNumber']
    tx_seq = -1
    act_seq = 0
    source = 'None'
    target = dict_action['author']
    amount = dict_action['value']
    parsed_entry = [directive, source, target, amount, tx, block_num, tx_seq, act_seq]
    return parsed_entry


def parse_action_suicide(dict_obj):
    directive = dict_obj['type']
    tx = dict_obj['transactionHash']
    block_num = dict_obj['blockNumber']
    tx_seq = dict_obj['transactionPosition']
    act_seq = 0
    dict_action = dict_obj['action']
    source = dict_action['refundAddress']
    target = dict_action['address']
    amount = dict_action['balance']
    parsed_entry = [directive, source, target, amount, tx, block_num, tx_seq, act_seq]
    return parsed_entry

# In[7]:


def fetch_entries_from_actions(actions):
    parsed_entries = []
    for act in actions:
        if act['type'] == 'call':
            parsed_entry = parse_action_call(act)
        elif act['type'] == 'create':
            parsed_entry = parse_action_create(act)
        elif act['type'] == 'reward':
            parsed_entry = parse_action_reward(act)
        elif act['type'] == 'suicide':
            print(act)
            parsed_entry = parse_action_suicide(act)
        else:
            print(act)
        if parsed_entry != None:
            parsed_entries.append(parsed_entry)
    last_tx_hash = ''
    act_seq = -1
    for en in parsed_entries:
        if en[4] != last_tx_hash:
            last_tx_hash=  en[4]
            act_seq = 0
        else:
            act_seq += 1
        en[7] = act_seq
    return parsed_entries


# In[8]:


def delete_error_actions(actions):
    err_txs_list = []
    for act in actions:
        if 'error' in act:
            err_txs_list.append(act['transactionHash'])
    tailored_actions_list = []
    for act in actions:
        if not act['transactionHash'] in err_txs_list:
            tailored_actions_list.append(act)
    return tailored_actions_list


# In[9]:


def parse_blocks(block_start, block_end, table_name):
#    print (block_start, block_end, table_name)
#    return
    for bn in tqdm(range(block_start, block_end)):
#         print ("######### Start to process block {} #########".format(bn))
        method = 'trace_block'
        params = [hex(bn)]
        actions_per_block = rpc_to_parity(method, params).json()['result']
#         print (actions_per_block)
        tailored_actions = delete_error_actions(actions_per_block)
        parsed_entries = fetch_entries_from_actions(tailored_actions)
        insert_multiple_actions(parsed_entries, table_name = table_name)


# In[11]:


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
