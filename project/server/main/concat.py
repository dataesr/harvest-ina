from pathlib import Path
import os
import sys
from project.server.main.logger import get_logger

logger = get_logger(__name__)

def concat(args):
    output_file = '/data/all_parsed_url.jsonl'
    root_path = Path('/data/url_parsed/')
    txt_files = list(root_path.glob("**/*.txt"))
    total_files = len(txt_files)
    logger.debug(f"Début de la concaténation de {total_files} fichiers...")
    processed = 0
    with open(output_file, 'w', encoding='utf-8') as outfile:
        for txt_file in sorted(txt_files):
            try:
                with open(txt_file, 'r', encoding='utf-8') as infile:
                    content = infile.read()
                    # Écrire le contenu
                    outfile.write(content)
                    # Ajouter un retour à la ligne si le contenu ne se termine pas déjà par \n
                    if not content.endswith('\n'):
                        outfile.write('\n')
                processed += 1
                if processed % 1000 == 0:
                    logger.debug(f"  Progression: {processed}/{total_files} fichiers traités")
            except Exception as e:
                logger.debug(f"Erreur lors de la lecture de {txt_file}: {e}")
                continue
    logger.debug(f"Terminé! {processed} fichiers concaténés dans {output_file}")
