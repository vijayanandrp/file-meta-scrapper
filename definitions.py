#!/usr/bin/env python3
import os
import random
import string
from datetime import datetime

def get_date_timestamp(fmt='%Y_%m_%d_%H_%M_%S'):
    date_stamp = datetime.now().strftime(fmt)
    print("date_stamp_with_time: %s " % date_stamp)
    return date_stamp

def random_string(length=6):
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))

root_dir = os.path.dirname(os.path.abspath(__file__))

# LOGGING
path_log = os.path.join(root_dir, 'logs')
if not os.path.isdir(path_log):
    os.makedirs(path_log)

file_name_log = '{}_meta_scrapper_{}.log'.format(get_date_timestamp(), random_string())

# SCAN MEDIA DIR
scan_dir = '/home/vijay-works/Downloads'

# Default Keyspace and table_names
db_key_space = 'master_db'
db_table_name = 'meta_data'

# CASSANDRA
CASSANDRA_SUPPORT = True
DROP_RECREATE = True  # DEV only
MAX_COLUMN_LIMIT = 50

# RERUN SCANNING AGAIN
DELETE_LOCK_FILES = True # DEV only
LOCK_FILE = 'SCANNED_META.LOCK'
