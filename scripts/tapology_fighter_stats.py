# %%
import json
import logging
import warnings
from datetime import datetime
from time import sleep

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# Configurações de Log e Alertas
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
warnings.filterwarnings("ignore")

DTM_TODAY = ""  # datetime.now().strftime("%Y%m%d%H%M%S")

FILE_TAPOLOGY_PVP_RANK = "../resources/tapology_ufc_pvp_rkng.json"
FILE_FIGHTERS = f"../../data/bronze/fighters_{DTM_TODAY}.csv"
FILE_FIGHTS = f"../../data/bronze/fights_{DTM_TODAY}.csv"


class Scraper:
    """Gerencia a instância do navegador e extração do HTML bruto."""

    def __init__(self):
        self.chrome_options = self._setup_options()
        self.driver = webdriver.Chrome(options=self.chrome_options)
        self.wait = WebDriverWait(self.driver, 20)

    def _setup_options(self) -> Options:
        options = Options()
        # options.add_argument("--headless")  # Descomente para rodar em background
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1280,720")
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/114.0.0.0 Safari/537.36"
        )
        options.page_load_strategy = "normal"
        return options

    def get_html(self, url: str) -> str:
        try:
            self.driver.get(url)
            # Espera explícita pelo carregamento do body
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            sleep(2)  # Buffer para scripts assíncronos
            return self.driver.page_source
        except Exception as e:
            logging.error(f"Erro ao acessar {url}: {e}")
            return ""

    def close(self):
        """Fecha a sessão do navegador ao final do scraping."""
        self.driver.quit()


class TapologyParser:
    """Classe especializada em converter HTML do Tapology em dicionários de dados."""

    def __init__(self, html_content: str):
        self.soup = BeautifulSoup(html_content, "html.parser")

    def parse_fighter_details(self) -> dict:
        details = {}
        container = self.soup.find("div", id="standardDetails")
        if container:
            for label in container.find_all("strong"):
                key = label.get_text(strip=True).replace(":", "")
                value_node = label.find_next_sibling(["span", "a"])
                details[key] = value_node.get_text(strip=True) if value_node else "N/A"
        return details

    def parse_mma_stats(self) -> dict:
        stats = {}
        # Record Geral
        record_tag = self.soup.select_one("div.bg-tap_e6 span:nth-of-type(2)")
        stats["Record"] = record_tag.get_text(strip=True) if record_tag else "N/A"

        # Métodos (KO, Sub, Dec, Disq)
        for stat_type in ["tko", "sub", "dec", "dis"]:
            tag = self.soup.find("li", id=f"{stat_type}RecordStats")
            if tag:
                sec = tag.find("div", class_="secondary")
                stats[f"{stat_type.upper()} Stats"] = (
                    sec.get_text(strip=True) if sec else "N/A"
                )
        return stats

    def parse_fight_results(self) -> list:
        results = []
        # Seleciona os blocos de lutas pelo atributo de ID de combate
        bouts = self.soup.find_all("div", attrs={"data-bout-id": True})

        for bout in bouts:
            # Uso de seletores flexíveis para evitar quebras por CSS dinâmico
            item = {
                "Status": "N/A",
                "Opponent": "N/A",
                "Event": "N/A",
                "Date": "N/A",
                "Method": "N/A",
                "Weight": "N/A",
            }

            res_tag = bout.select_one(".result .text-white")
            if res_tag:
                item["Status"] = res_tag.get_text(strip=True)

            opp_tag = bout.find("a", href=lambda x: x and "/fighters/" in x)
            if opp_tag:
                item["Opponent"] = opp_tag.get_text(strip=True)

            event_tag = bout.find("a", href=lambda x: x and "/events/" in x)
            if event_tag:
                item["Event"] = event_tag.get_text(strip=True)

            date_tag = bout.find("span", class_=lambda x: x and "text-tap_3" in x)
            if date_tag:
                item["Date"] = date_tag.get_text(strip=True)

            results.append(item)
        return results


def main():
    # Carregar lista de entrada
    try:
        with open(FILE_TAPOLOGY_PVP_RANK, "r", encoding="utf-8") as f:
            fighters_to_scrape = json.load(f)
    except FileNotFoundError:
        logging.error("Arquivo JSON de entrada não encontrado.")
        return

    # Inicializa o Scraper apenas uma vez
    scraper = Scraper()

    # Flags para cabeçalho
    first_fighter = True
    first_fight = True

    try:
        for entry in fighters_to_scrape:
            name = entry.get("fighter")
            url = entry.get("url_tapology")

            logging.info(f"Processando: {name}")

            html = scraper.get_html(url)

            if not html:
                logging.warning(f"Falha ao obter HTML para {name}")
                continue

            parser = TapologyParser(html)

            # Extrair dados
            details = parser.parse_fighter_details()
            details["Fighter Name"] = name  # Garantir chave primária

            fights = parser.parse_fight_results()
            for f in fights:
                f["Fighter"] = name  # Linkar lutas ao lutador

            # Salvar imediatamente no CSV dos lutadores
            df_fighter = pd.DataFrame([details])
            df_fighter.to_csv(
                FILE_FIGHTERS,
                mode="a",
                index=False,
                header=first_fighter,
                encoding="utf-8",
            )
            first_fighter = False

            # Salvar as lutas
            if fights:
                df_fights = pd.DataFrame(fights)
                df_fights["Main Fighter"] = (
                    name  # Garantir que a coluna "Main Fighter" seja adicionada aqui
                )
                df_fights.to_csv(
                    FILE_FIGHTS,
                    mode="a",
                    index=False,
                    header=first_fight,
                    encoding="utf-8",
                )
                first_fight = False

            # Respeito ao servidor
            sleep(1)

    finally:
        scraper.close()

    logging.info("Done.")


if __name__ == "__main__":
    main()
