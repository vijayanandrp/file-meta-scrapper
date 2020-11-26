#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pyexifinfo as exif
import os
import json
import random
import string
import uuid
from datetime import datetime
from progress.bar import Bar
from definitions import scan_dir, CASSANDRA_SUPPORT, DROP_RECREATE, \
DELETE_LOCK_FILES, LOCK_FILE, MAX_COLUMN_LIMIT, db_key_space, db_table_name
from lib_cassandra import Cassandra
from logger import Logger
import hashlib
import pandas as pd
import shutil

# This is the exiftool processor, it runs exiftool and puts the outputs in either
# html, json or XML depending on which function is called Exiftool
# needs to be installed for this to work.

target_path = '.'

log = Logger(file_name=None, level='INFO',
             stream_output=True).defaults(name_class='main')


def id_generator(length=8):
    return ''.join(random.SystemRandom().choice(
        string.ascii_uppercase + string.digits) for _ in range(length))


def generate_source_id(n=16):
    return id_generator(n)


def generate_artifacts_id():
    return str(uuid.uuid4())


# Scan all directories and return the full paths
def scan_directories(target_path):
    target_files = []
    for r, d, f in os.walk(target_path):
        for file in f:
            target_files.append(os.path.join(r, file))
    return target_files

artifact_id_prefix = "A"
source_id_prefix = "S"

# exiftool extracts meta data returns as JSON
def exifJSON(target_files, source_id):
    mediafiles = len(target_files)
    jsonbar = Bar('MetaData Processing ', max=mediafiles)
    metadata_info = []
    for idx, filename in enumerate(target_files):
        idx  = idx + 1
        log.info('[*] >> Extracting data from file - {}'.format(filename))
        if LOCK_FILE in filename:
            continue
        try:
            exifoutputjson = exif.get_json(filename)
            exifoutputjson[0]['artifact_id'] = source_id + artifact_id_prefix + str(idx)
            exifoutputjson[0]['source_id'] = source_id
            exifoutputjson[0]['md5_hash'] = hashlib.md5(open(filename, 'rb').read()).hexdigest()
            jsonbar.next()
            log.debug(json.dumps(
                exifoutputjson[0], sort_keys=True, indent=0, separators=(',', ': ')))
            if len(exifoutputjson[0]) > MAX_COLUMN_LIMIT:
                log.error('Max column size exceeded!- {}'.format(filename) )
                continue
            metadata_info.append(exifoutputjson[0])
        except Exception as error:
            log.error('[-]Error when reading meta data for {}'.format(filename))
            log.error('[-] {}'.format(str(error)))
    jsonbar.finish()
    return metadata_info


if __name__ == '__main__':
    log.info('>> [*] Running the exiftool scanner')
    if not scan_dir:
        while True:
            scan_dir = input(">> [+] Enter the targer directory to scan - ")
            if not os.path.isdir(scan_dir):
                log.info('>> [-] Invalid Directory. Please try again or press Ctrl + C to quit')
                continue
            else:
                scan_dir = os.path.abspath(scan_dir)
                break

    source_dirs = [os.path.join(scan_dir, _) for _ in os.listdir(scan_dir)
                   if os.path.isdir(os.path.join(scan_dir, _))]

    c = None
    if CASSANDRA_SUPPORT:
        c = Cassandra()
        c.create_keyspace(key_space=db_key_space)
        c.create_table(table_name=db_table_name)

        if DROP_RECREATE:
            log.info('>> [!] DROP & RECREATE DB ')
            c.truncate_table()
            c.drop_table()
            c.drop_keyspace()
            c.create_keyspace()
            c.create_table()

    dataframes  = []
    for idx, source_dir in enumerate(source_dirs):
        idx = idx + 1
        print("**" * 50)
        target_files = scan_directories(source_dir)
        scanned_lock = os.path.join(source_dir, LOCK_FILE)

        if not target_files:
            log.info('>> [-] Target files not found in {}'.format(source_dir))
            continue

        if DELETE_LOCK_FILES and os.path.isfile(scanned_lock):
            log.info('>> [!] DELETE LOCK FILES - {}'.format(scanned_lock))
            os.remove(scanned_lock)

        if os.path.isfile(scanned_lock):
            log.info('>> [!] Lock file found. Assumes Source is already scanned.')
            lines = []
            with open(scanned_lock) as fp:
                lines = fp.readlines()
            log.info('>>[+] {}'.format(' '.join(lines)))
            continue

        log.info('>> [+] Started processing source dir - ' + source_dir)
        source_id = source_id_prefix + str(idx)
        metadata_info = exifJSON(target_files, source_id=source_id)
        df = pd.DataFrame.from_records(metadata_info)

        dataframes.append({'df': df, 'source_dir': os.path.basename(source_dir)})

        if CASSANDRA_SUPPORT and c:
            [c.insert_json(_info) for _info in metadata_info]

        with open(scanned_lock, 'w') as fp:
            fp.write("Completed scanning for source_id {} during date - {}".format(
                source_id, datetime.now().strftime("%A, %d. %B %Y %I:%M%p")))
            fp.write('\n')

    excel_file = "Metadata_All.xlsx"
    if os.path.isfile(excel_file):
        shutil.rmtree(excel_file)
    print(datetime.now().strftime("%A_%d_%B_%Y_%I_%M%p"))
    with pd.ExcelWriter(excel_file) as writer:
        for dataframe in dataframes:
            _df = dataframe['df']
            _columns = list(_df.columns)
            _columns.sort()
            _new_columns =  ['SourceFile', 'artifact_id', 'source_id', 'md5_hash', 'File:Directory',
                             'File:FileAccessDate', 'File:FileInodeChangeDate', 'File:FileModifyDate',
                             'File:FileName', 'File:FilePermissions', 'File:FileSize', 'File:FileType',
                             'File:FileTypeExtension', 'File:MIMEType', 'ExifTool:ExifToolVersion']
            for x in _new_columns:
                _columns.remove(x)
            _columns = _new_columns + _columns
            _df = _df[_columns]
            _df.to_excel(writer, sheet_name=dataframe['source_dir'], index = False)

    log.info('>> Total files scanned - {}'.format(len(target_files)))
    log.info('>> [+] -*- Completed scanning -*- ')
