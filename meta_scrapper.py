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
from definitions import scan_dir, CASSANDRA_SUPPORT, DROP_RECREATE, DELETE_LOCK_FILES, LOCK_FILE, db_key_space, db_table_name
from lib_cassandra import Cassandra
from logger import Logger

# This is the exiftool processor, it runs exiftool and puts the outputs in either
# html, json or XML depending on which function is called Exiftool
# needs to be installed for this to work.

target_path = '.'

log = Logger(file_name=None, level='INFO', stream_output=True).defaults(name_class='main')

def id_generator(length=8):
    return ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(length))


def generate_source_id():
    return id_generator(16)


def generate_artifacts_id():
    return str(uuid.uuid4())


def scan_directories(target_path):
    target_files = []
    for r, d, f in os.walk(target_path):
        for file in f:
            target_files.append(os.path.join(r, file))
    return target_files


# exiftool in JSON
def exifJSON(target_files, source_id, cassandra_obj=None):
    mediafiles = len(target_files)
    jsonbar = Bar('MetaData Processing ', max=mediafiles)
    for i in range(mediafiles):
        for filename in target_files:
            log.info('[*] >> Extracting data from file - {}'.format(filename))
            if LOCK_FILE in filename:
                continue
            try:
                exifoutputjson = exif.get_json(filename)
                exifoutputjson[0]['artifact_id'] = generate_artifacts_id()
                exifoutputjson[0]['source_id'] = source_id
                jsonbar.next()
                log.debug(json.dumps(exifoutputjson[0], sort_keys=True, indent=0, separators=(',', ': ')))
                if cassandra_obj:
                    cassandra_obj.insert_json(exifoutputjson[0])
            except Exception as error:
                log.info('[-]Error while reading meta data for {}'.format(filename))
                log.info('[-] {}'.format(str(error)))

        break
    jsonbar.finish()
    return source_id


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

    if c and DROP_RECREATE:
        log.info('>> [!] DROP & RECREATE DB ')
        c.truncate_table()
        c.drop_table()
        c.drop_keyspace()
        c.create_keyspace()
        c.create_table()

    for source_dir in source_dirs:
        target_files = scan_directories(source_dir)

        if not target_files:
            log.info('>> [-] Target files not found in {}'.format(source_dir))
            continue

        scanned_lock = os.path.join(source_dir, LOCK_FILE)
        if DELETE_LOCK_FILES and os.path.isfile(scanned_lock):
            log.info('>> [!] DELETE LOCK FILES - {}'.format(scanned_lock))
            os.remove(scanned_lock)

        if os.path.isfile(scanned_lock):
            lines = []
            with open(scanned_lock) as fp:
                lines = fp.readlines()
            log.info('>>[+] {}'.format(' '.join(lines)))
            continue

        log.info('>> [+] Started processing source dir - ' + source_dir)

        source_id = generate_source_id()
        exifJSON(target_files, source_id=source_id, cassandra_obj=c)

        with open(scanned_lock, 'w') as fp:
            fp.write("Completed scanning for source_id {} during date - {}".format(
                source_id, datetime.now().strftime("%A, %d. %B %Y %I:%M%p")))
            fp.write('\n')

    log.info('>> Total files scanned - {}'.format(len(target_files)))
    log.info('>> [+] -*- Completed scanning -*- ')
