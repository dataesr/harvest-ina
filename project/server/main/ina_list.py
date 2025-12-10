from bs4 import BeautifulSoup
import os
import pandas as pd
from project.server.main.parse import parse_ina
from project.server.main.utils import get_data_ina, get_filepath, to_jsonl
from project.server.main.logger import get_logger

logger = get_logger(__name__)

def list_urls(args: dict) -> None:
    full_names = args.get('full_names')
    use_cache = args.get('use_cache', False)
    assert(isinstance(full_names, list))
    for full_name in full_names:
        current_file = get_data_ina_list(full_name, use_cache)
        df_tmp = pd.read_json(current_file, lines=True)
        for ix, e in enumerate(df_tmp.to_dict(orient='records')):
            save_cache = False
            if ix == len(df_tmp)-1:
                save_cache = True
            parse_ina(url = e['url'], save_cache=save_cache)            

def get_data_ina_list(full_name, use_cache = False):
    full_name = full_name.lower().strip()
    current_file = get_filepath(full_name, 'full_name')
    if use_cache and os.path.isfile(current_file):
        return current_file
    url = f'https://catalogue.ina.fr/docListe/TV-RADIO/?base_label=TVNAT%2CRADIO%2CTVSAT%2CPUBTV%2CTVREG%2CCLIPTV%2CDLDON&sujets_filter=Sujet&gen="{full_name}"&bool_operator=AND&tri=score1&nbLignes=500'
    logger.debug(url)
    txt = get_data_ina(url)
    data = parse_data_ina_list(txt)
    to_jsonl(data, current_file)
    return current_file
    
def parse_data_ina_list(txt):
    data = []
    soup = BeautifulSoup(txt, 'html.parser')
    trs = soup.find_all('tr', class_='result_line_a') + soup.find_all('tr', class_='result_line_b')
    for tr in trs:
        try:
            date = tr.find('td', class_='date_column').get_text()
            url = tr.find('a').attrs['href'].split('?rang')[0]
            data.append({'url': url, 'date': date, 'year': date[-4:]})
        except:
            logger.debug(f'error with {tr}')
            continue
    logger.debug(f'{len(data)} lines detected')
    return data





