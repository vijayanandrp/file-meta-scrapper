#!/usr/bin/env python3

import pyexifinfo as exif
import os
import json
from progress.bar import Bar

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
    jsonbar = Bar('Processing', max=mediafiles)
    for i in range(mediafiles):
        for filename in target_files:
            exifoutputjson = exif.get_json(filename)
            meta_datajson.append(exifoutputjson[0])
            print(json.dumps(exifoutputjson, sort_keys=True, indent=0, separators=(',', ': ')))
            jsonbar.next()
        break
    jsonbar.finish()
    print(len(meta_datajson))
    with open('final_metadata.json', 'w', encoding='utf-8') as f:
        json.dump(meta_datajson, f, ensure_ascii=False, indent=4)


if __name__ == '__main__':
    print('Running the exiftool scanner')
    target_path = '/home/vijay-works/Documents/Metaforge-master'
    target_files = scan_directories(target_path)
    if not target_files:
        print('Target files not found.')
        exit()
    exifJSON(target_files)
    print(' -*- Completed -*- ')
