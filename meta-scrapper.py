#!/usr/bin/env python3

import pyexifinfo as exif
import os
import json
from progress.bar import Bar
from definitions import scan_dir

# This is the exiftool processor, it runs exiftool and puts the outputs in either
# html, json or XML depending on which function is called Exiftool
# needs to be installed for this to work.

target_path = '.'


def scan_directories(target_path):
    target_files = []
    for r, d, f in os.walk(target_path):
        for file in f:
            target_files.append(os.path.join(r, file))
    return target_files


# exiftool in JSON
def exifJSON(target_files):
    meta_datajson = list()
    mediafiles = len(target_files)
    jsonbar = Bar('MetaData Processing', max=mediafiles)
    for i in range(mediafiles):
        for filename in target_files:
            exifoutputjson = exif.get_json(filename)
            meta_datajson.append(exifoutputjson[0])
            jsonbar.next()
            print(json.dumps(exifoutputjson, sort_keys=True, indent=0, separators=(',', ': ')))
        break
    jsonbar.finish()



if __name__ == '__main__':
    print('[*] Running the exiftool scanner')
    if not scan_dir:
        while True:
            scan_dir = input(">> [+] Enter the targer directory to scan - ")
            if not os.path.isdir(scan_dir):
                print('>> [-] Invalid Directory. Please try again or press Ctrl + C to quit')
                continue
            else:
                scan_dir = os.path.abspath(scan_dir)
                break

    target_files = scan_directories(scan_dir)
    if not target_files:
        print('>> [-] Target files not found.')
        exit()
    exifJSON(target_files)
    print('>> Total files scanned - {}'.format(len(target_files)))
    print('>> [+] -*- Completed scanning -*- ')
