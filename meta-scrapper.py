#!/usr/bin/env python3

import pyexifinfo as exif
import os
import json
from progress.bar import Bar
from definitions import scan_dir
import random
import string
import uuid

# This is the exiftool processor, it runs exiftool and puts the outputs in either
# html, json or XML depending on which function is called Exiftool
# needs to be installed for this to work.

target_path = '.'

def id_generator(length=8):
    return ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(length))

def generate_source_id():
    return id_generator(8)

def generate_artifacts_id():
    return str(uuid.uuid4())

def scan_directories(target_path):
    target_files = []
    for r, d, f in os.walk(target_path):
        for file in f:
            target_files.append(os.path.join(r, file))
    return target_files


# exiftool in JSON
def exifJSON(target_files, source_id):
    meta_datajson = list()
    mediafiles = len(target_files)
    jsonbar = Bar('MetaData Processing', max=mediafiles)
    for i in range(mediafiles):
        for filename in target_files:
            exifoutputjson = exif.get_json(filename)
            exifoutputjson[0]['artifact_id'] = generate_artifacts_id()
            exifoutputjson[0]['source_id'] = source_id
            meta_datajson.append(exifoutputjson[0])
            jsonbar.next()
            print(json.dumps(exifoutputjson, sort_keys=True, indent=0, separators=(',', ': ')))
        break
    jsonbar.finish()



if __name__ == '__main__':
    print('>> [*] Running the exiftool scanner')
    if not scan_dir:
        while True:
            scan_dir = input(">> [+] Enter the targer directory to scan - ")
            if not os.path.isdir(scan_dir):
                print('>> [-] Invalid Directory. Please try again or press Ctrl + C to quit')
                continue
            else:
                scan_dir = os.path.abspath(scan_dir)
                break

    source_dirs  = [os.path.join(scan_dir, _) for _ in os.listdir(scan_dir)
                   if os.path.isdir(os.path.join(scan_dir, _))]
    for source_dir in source_dirs:
        target_files = scan_directories(source_dir)
        if not target_files:
            print('>> [-] Target files not found in {}'.format(source_dir))
            continue
        print('>> [+] Started processing source dir - ', source_dir)
        exifJSON(target_files, source_id=generate_source_id())

    print('>> Total files scanned - {}'.format(len(target_files)))
    print('>> [+] -*- Completed scanning -*- ')
