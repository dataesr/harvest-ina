import pickle
import re
import os
import shutil
import datetime
import requests
import base64
import time
import hashlib
import fasttext
import orjson
import json
from retry import retry
from project.server.main.logger import get_logger


logger = get_logger(__name__)

def hash_txt(txt):
    return hashlib.sha256(txt.encode("utf-8")).hexdigest()

def get_filepath(txt, type_):
    hash_ = hash_txt(txt)
    os.system(f'mkdir -p /data/{type_}/{hash_[-2:]}')
    return f'/data/{type_}/{hash_[-2:]}/{hash_}.txt'

@retry(delay=300, tries=5, logger=logger)
def get_data_ina(url):
    res = None
    #logger.debug(f'{nb_rows_total} and new url {url}')
    r = requests.get(url, timeout=100)
    #logger.debug(f'status_code : {r.status_code}')
    try:
        res = r.text
    except:
        logger.debug(f'ERROR for url {url}')
        logger.debug(r.status_code)
        logger.debug(r.text)
    return res

def clean_json(elt):
    keys = list(elt.keys()).copy()
    for f in keys:
        if isinstance(elt[f], dict):
            elt[f] = clean_json(elt[f])
        elif (not elt[f] == elt[f]) or (elt[f] is None):
            del elt[f]
        elif (isinstance(elt[f], str) and len(elt[f])==0):
            del elt[f]
        elif (isinstance(elt[f], list) and len(elt[f])==0):
            del elt[f]
    return elt

def to_jsonl(input_list, output_file, mode = 'a'):
    with open(output_file, mode) as outfile:
        for entry in input_list:
            new = clean_json(entry)
            json.dump(new, outfile)
            outfile.write('\n')

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def get_ip():
    ip = requests.get('https://api.ipify.org').text
    return ip

def download_from_s3(distant_file, local_path):
    cmd = f'aws s3 cp s3://ina/{distant_file} {local_path}'
    logger.debug(f'download_from_s3 {cmd}')
    os.system(cmd)

def cp_folder_local_s3(folder_local, folder_distant=None):
    if folder_distant is None:
        folder_distant = folder_local
    cmd = f'aws s3 cp {folder_local} s3://ina/{folder_distant}  --recursive'
    logger.debug(f'cp_folder_local_s3 for {folder_local} to {folder_distant} cmd={cmd}')
    os.system(cmd)

def get_path_from_id(id):
    s1 = id[-2:].lower()
    s2 = id[-4:-2].lower()
    s3 = id[-6:-4].lower()
    s4 = id[-8:-6].lower()
    return f'{s1}/{s2}/{s3}/{s4}'

def is_dowloaded(elt_id):
    filename = get_filename(elt_id, 'grobid')
    return os.path.isfile(filename)
