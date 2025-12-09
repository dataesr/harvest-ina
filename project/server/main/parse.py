from bs4 import BeautifulSoup
import os
import json
import pickle
import time
from project.server.main.utils import get_data_ina, get_filepath, to_jsonl, hash_txt
from project.server.main.logger import get_logger

logger = get_logger(__name__)

KNOWN_URLS = set()

def get_cache():
    try:
        KNOWN_URLS = pickle.load(open('/data/KNOWN_URLS.pkl', 'rb'))
    except:
        pass
    logger.debug(f'{len(KNOWN_URLS)} KNOWN_URLS loaded')
    return KNOWN_URLS

def parse_ina(url, save_cache=False):
    global KNOWN_URLS
    if len(KNOWN_URLS)==0:
        KNOWN_URLS = get_cache()
    hash_url = hash_txt(url)
    if hash_url in KNOWN_URLS:
        return
    logger.debug(url)
    res = {'url': url}
    txt = get_data_ina(url)
    current_file = get_filepath(url, 'url')
    with open(current_file, "w", encoding="utf-8") as f:
        f.write(txt)
    soup = BeautifulSoup(txt, 'html.parser')
    for g in FIELD_DICT.keys():
        current_elt = soup.find(attrs={"id": g})
        if current_elt:
            res[FIELD_DICT[g]] = current_elt.get_text(' ')
            if g=='GEN':
                res['participants'] = parse_credits(res[FIELD_DICT[g]])
    current_file_parsed = get_filepath(url, 'url_parsed')
    json.dump(res, open(current_file_parsed, 'w'))
    KNOWN_URLS.add(hash_url)
    if save_cache:
        logger.debug(f'saving {len(KNOWN_URLS)} KNOWN_URLS.pkl')
        pickle.dump(KNOWN_URLS, open('/data/KNOWN_URLS.pkl', 'wb'))
    time.sleep(0.5)
    return res

def parse_name_role_simple(text):
    """
    Version plus simple sans regex.
    """
    if '(' in text and ')' in text:
        # Trouver la position des parenthèses
        start_paren = text.find('(')
        end_paren = text.find(')')

        name = text[:start_paren].strip()
        role = text[start_paren+1:end_paren].strip()

        return {'name': name, 'role': role}
    else:
        return {'name': text.strip()}

def parse_credits(t):
    participants = []
    for e in t.split(';'):
        txt = e.strip()
        if txt.startswith('PAR,'):
            for par in txt.split(','):
                new_elt = parse_name_role_simple(txt[4:])
                if new_elt not in participants:
                    participants.append(new_elt)
    return participants

FIELD_DICT = {'NO': 'id_notice',
 'TI': 'title',
 'TICOL': 'collection_title',
 'TIPROG': 'program_title',
 'CH': 'diffusion_canal',
 'DATDIF': 'diffusion_date',
 'JOUR': 'diffusion_day',
 'STADIF': 'diffusion_status',
 'HEURE': 'diffusion_time',
 'HEUREFIN': 'diffusion_end_time',
 'DUREE': '́length',
 'GENRE': 'genre',
 'TYPDESCR': 'description_type',
 'GEN': 'credits',
 'DE': 'descriptors',
 'CHAP': 'Chapeau',
 'RES': 'documentary_summary',
 'RESPROD': 'producer_summary',
 'SP': 'programs_society',
 'NATPRO': 'production_type',
 'PROD': 'producers',
 'GEO': 'geo_extension',
 'SELDL': 'dl_selection',
 'DL': 'df_number',
 'BASE': 'base',
 'FONDS': 'fund',
 'TIDL': 'material_title'}
