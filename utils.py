import requests
import datetime
import pytz
import pymysql
from multiprocessing import Pool


rpc_ip = "http://localhost"
rpc_port = 8545

db_config = {
    'host' : "127.0.0.1",
    'port' : 8306,
    'user' : 'eth',
    'password' : '123456',
    'db' : 'eth'
}

lowest_bn = 1

def hex2wei(s):
    return int(s, 16)

def wei2eth(w):
    return w/10**18

# return the 1st timestamp of day
def date_to_timestamp(year, month, day, tz_str='utc'):
    dt = datetime.datetime(year, month, day)
    tz = None
    if tz_str=='utc':
        tz = pytz.utc
    else:
        tz = pytz.timezone(tz_str)
    dt_tz = tz.localize(dt)
    return dt_tz.timestamp()

def timestamp_to_date(ts, tz_str='utc'):
    tz = None
    if tz_str=='utc':
        tz = pytz.utc
    else:
        tz = pytz.timezone(tz_str)
    dt = datetime.datetime.fromtimestamp(ts, tz)
    return dt

def rpc_to_parity(method, params):
    rpc_ip_port = rpc_ip + ":" + str(rpc_port)
    payload = {"jsonrpc":"2.0",
              "method":method,
              "params":params,
              "id":1}
    headers = {'Content-type': 'application/json'}
    session = requests.Session()
    response = session.post(rpc_ip_port, json=payload, headers=headers)
    return response


def query_timestamp_of_block(block_num):
    method = 'eth_getBlockByNumber'
    params = [hex(block_num), True]
    rpc_resp = rpc_to_parity(method, params)
    ts_hex = rpc_resp.json()['result']['timestamp']
    return int(ts_hex, 16)


def query_highest_blocknumber():
    method = 'eth_blockNumber'
    params = []
    rpc_resp = rpc_to_parity(method, params)
    bn_hex = rpc_resp.json()['result']
    return int(bn_hex, 16)

def check_date_reasonable(year, month, day):
    highest_bn = query_highest_blocknumber()
    highest_ts = query_timestamp_of_block(highest_bn)
    highest_dt = timestamp_to_date(highest_ts)
    lowest_ts = query_timestamp_of_block(lowest_bn)
    ts = date_to_timestamp(year, month, day)
    if ts < lowest_ts:
        print("Date {}/{}/{} is too early!".format(year, month, day))
        return False
    if ts > highest_ts:
        print("""Date {}/{}/{} is too new, the newest date synchronized is
                {}/{}/{} !""".format(year, month, day, highest_dt.year, highest_dt.month, highest_dt.day))
        return False
    return True

# Two steps
# Step 1: Shorten the possible range
# Step 2: Iterate from the lower bound of range

def first_block_of_day(year, month, day):
    if not check_date_reasonable(year, month, day):
        return
    highest_bn = query_highest_blocknumber()
    highest_ts = query_timestamp_of_block(highest_bn)
    lowest_bn = 1
    lowest_ts = query_timestamp_of_block(lowest_bn)
    target_ts = date_to_timestamp(year, month, day)
    # Step 1: Shorten the possible range
    while(True):
        tmp_bn = int(lowest_bn + (highest_bn-lowest_bn)*(target_ts-lowest_ts)/(highest_ts-lowest_ts))
        tmp_ts = query_timestamp_of_block(tmp_bn)
        if (tmp_ts > target_ts):
            highest_ts = tmp_ts
            highest_bn = tmp_bn
        elif (tmp_ts < target_ts):
            lowest_ts = tmp_ts
            lowest_bn = tmp_bn
        else:
            return tmp_bn
        if (tmp_bn == int(lowest_bn + (highest_bn-lowest_bn)*(target_ts-lowest_ts)/(highest_ts-lowest_ts))):
            break
    # Step 2: Iterate from the lower bound of range
    tmp_ts = lowest_ts
    tmp_bn = lowest_bn
    while(tmp_ts < target_ts):
        tmp_bn += 1
        tmp_ts = query_timestamp_of_block(tmp_bn)
    return tmp_bn
        
# Two steps
# Step 1: Shorten the possible range
# Step 2: Iterate from the upper bound of range
def last_block_of_day(year, month, day):
    if not check_date_reasonable(year, month, day):
        return
    highest_bn = query_highest_blocknumber()
    highest_ts = query_timestamp_of_block(highest_bn)
    lowest_bn = 1
    lowest_ts = query_timestamp_of_block(lowest_bn)
    
    dt = datetime.datetime(year, month, day)
    dt_next = dt + datetime.timedelta(days=1)
    target_ts = date_to_timestamp(dt_next.year, dt_next.month, dt_next.day)-1
    # Step 1: Shorten the possible range
    while(True):
        tmp_bn = int(lowest_bn + (highest_bn-lowest_bn)*(target_ts-lowest_ts)/(highest_ts-lowest_ts))
        tmp_ts = query_timestamp_of_block(tmp_bn)
        if (tmp_ts > target_ts):
            highest_ts = tmp_ts
            highest_bn = tmp_bn
        elif (tmp_ts < target_ts):
            lowest_ts = tmp_ts
            lowest_bn = tmp_bn
        else:
            return tmp_bn
        if (tmp_bn == int(lowest_bn + (highest_bn-lowest_bn)*(target_ts-lowest_ts)/(highest_ts-lowest_ts))):
            break
    # Step 2: Iterate from the upper bound of range
    tmp_ts = lowest_ts
    tmp_bn = lowest_bn
    while(tmp_ts <= target_ts):
        tmp_bn += 1
        tmp_ts = query_timestamp_of_block(tmp_bn)
    tmp_bn -= 1
    return tmp_bn
    

# Split a large list into several smaller sub-lists and then run the function with each sub_list as argument
# arg1: list object
# arg2: number of parallel progresses
# arg3: a function with list as argument
def map_list(large_list, par, func):
    sublist_len = len(large_list) // par
    sublists = []
    for i in range(par-1):
        tmp_list = large_list[i*sublist_len:(i+1)*sublist_len]
        sublists.append(tmp_list)
    tmp_list = large_list[(par-1)*sublist_len:]
    sublists.append(tmp_list)
    p = Pool(par)
    results = p.map(func, sublists)
    p.close()
    return results    


# There are many groups of arguments and each group contains multiple arguments
# arg1: list of args group
# arg2: a function with multiple arguments
def map_args_group(args_group_list, func):
    par = len(args_group_list)
    p = Pool(par)
    results = p.starmap(func, args_group_list)
    p.close()
#     print ("{} has been finished".format(args_group_list))
    return results

    
######################################################################
######################## Operate the database #######################
######################################################################

def exeSQL(sql, commit=False):
    connection = pymysql.connect(**db_config)
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
        if commit:
            connection.commit()
        else:
            result = cursor.fetchall()
            return result
    finally:
        connection.close()
    
    
def exeMultipleSQL(sqls, commit=False):
    connection = pymysql.connect(**db_config)
    try:
        with connection.cursor() as cursor:
            for sql in sqls:
                cursor.execute(sql)
        if commit:
            connection.commit()
        else:
            result = cursor.fetchall()
            return result
    finally:
        connection.close()    
    

def insert_action(directive, source, target, amount, tx, block_num, tx_seq, act_seq, table_name='action_20161001_20161231'):
    insert_act_sql = """INSERT INTO {} (directive, source, target, amount, tx, block_num, tx_seq, act_seq) 
            VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')""".format(table_name, directive, source,
                                                  target, amount, tx, block_num, tx_seq, act_seq)
    print(insert_act_sql)
    exeSQL(insert_act_sql, True)
    
    
def insert_multiple_actions(parsed_entries, table_name='action_20161001_20161231'):
    insert_act_sqls = []
    for en in parsed_entries:
        sql = """INSERT INTO {} (directive, source, target, amount, tx, block_num, tx_seq, act_seq) 
            VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')""".format(table_name, *en)
        insert_act_sqls.append(sql)
    exeMultipleSQL(insert_act_sqls, True)
    
def create_action_table(table_name):
    create_action_sql = """CREATE TABLE {} ( `act_id` int(11) NOT NULL AUTO_INCREMENT,
                `directive` enum('call','create','reward-block','reward-uncle','suicide') NOT NULL, 
                `source` char(42) NOT NULL, `target` char(42) NOT NULL, `amount` varchar(32) NOT NULL, 
                `tx` char(66) NOT NULL, `block_num` int(11) NOT NULL, `tx_seq` int(11) NOT NULL, 
                `act_seq` int(11) NOT NULL, PRIMARY KEY (`act_id`), 
                UNIQUE KEY `unique_action` (`block_num`,`tx_seq`,`act_seq`), KEY `block_num_index` (`block_num`), 
                FULLTEXT `target_index` (`target`), FULLTEXT `source_index` (`source`), 
                FULLTEXT `tx_index` (`tx`) ) ENGINE=InnoDB""".format(table_name)
    exeSQL(create_action_sql, True)
    
def drop_table(table_name):
    drop_table_sql = """DROP TABLE {}""".format(table_name)
    exeSQL(drop_table_sql, True)
    
def insert_account(address, account_type, table_name):
    insert_act_sql = """INSERT INTO {} (address, kind) 
            VALUES ('{}', '{}')""".format(table_name, address, account_type)
#     print(insert_act_sql)
    exeSQL(insert_act_sql, True)