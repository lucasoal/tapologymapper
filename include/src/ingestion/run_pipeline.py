import json
import os

import pandas as pd

from .parser import TapologyParser
from .scraper_engine import ScraperEngine


def get_fighters_list(**kwargs):  # O **kwargs "limpa" os argumentos automáticos do Airflow
    import json
    import os

    base_path = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
    json_path = os.path.join(base_path, "include/src/resources/tapology_ufc_pvp_rkng.json")

    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)


def scrape_single_fighter(fighter_data):
    engine = ScraperEngine()
    try:
        html = engine.get_html(fighter_data["url_tapology"])
        if not html:
            return None
        parser = TapologyParser(html)
        return parser.parse_fighter_details(fighter_data["fighter"])
    finally:
        engine.close()
